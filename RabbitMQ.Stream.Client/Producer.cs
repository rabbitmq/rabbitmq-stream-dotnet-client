using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public struct Confirmation
    {
        public ulong PublishingId { get; set; }
        public ResponseCode Code { get; set; }
    }

    public record ProducerConfig
    {
        public string Stream { get; set; }
        public string Reference { get; set; }
        public int MaxInFlight { get; set; } = 1000;
        public Action<Confirmation> ConfirmHandler { get; set; } = _ => { };

        public Func<string, Task> ConnectionClosedHandler { get; set; }
    }

    public class Producer : AbstractEntity, IDisposable
    {
        private bool _disposed;
        private byte publisherId;
        private readonly ProducerConfig config;
        private readonly Channel<OutgoingMsg> messageBuffer;
        private readonly SemaphoreSlim semaphore;

        public int PendingCount => config.MaxInFlight - semaphore.CurrentCount;

        private readonly ConcurrentQueue<TaskCompletionSource> flows = new();
        private Task publishTask;

        private Producer(Client client, ProducerConfig config)
        {
            this.client = client;
            this.config = config;
            this.messageBuffer = Channel.CreateBounded<OutgoingMsg>(new BoundedChannelOptions(10000)
            {
                AllowSynchronousContinuations = false, SingleReader = true, SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
            this.publishTask = Task.Run(ProcessBuffer);
            this.semaphore = new(config.MaxInFlight, config.MaxInFlight);
        }

        public int MessagesSent => client.MessagesSent;
        public int ConfirmFrames => client.ConfirmFrames;
        public int IncomingFrames => client.IncomingFrames;
        public int PublishCommandsSent => client.PublishCommandsSent;

        private async Task Init()
        {
            client.ConnectionClosed += async (reason) =>
            {
                if (config.ConnectionClosedHandler != null)
                {
                    await config.ConnectionClosedHandler(reason);
                }
            };

            var (pubId, response) = await client.DeclarePublisher(
                config.Reference,
                config.Stream,
                publishingIds =>
                {
                    foreach (var id in publishingIds.Span)
                    {
                        config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = ResponseCode.Ok,
                        });
                    }

                    semaphore.Release(publishingIds.Length);
                },
                errors =>
                {
                    foreach ((ulong id, ResponseCode code) in errors)
                    {
                        config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = code,
                        });
                    }

                    semaphore.Release(errors.Length);
                });

            if (response.ResponseCode == ResponseCode.Ok)
            {
                publisherId = pubId;
                return;
            }

            throw new CreateProducerException($"producer could not be created code: {response.ResponseCode}");
        }

        /// <summary>
        /// SubEntry Batch send: Aggregate more messages under the same publishingId.
        /// Relation is publishingId ->[]messages. 
        /// Messages can be compressed using different methods.
        /// </summary>
        /// <param name="publishingId"></param>
        /// <param name="subEntryMessages"> List of messages for sub-entry. Max len allowed is ushort.MaxValue</param>
        /// <param name="compressionType">No Compression, Gzip Compression. Other types are not provided by default</param>
        public async ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
        {
            if (subEntryMessages.Count != 0)
            {
                await SemaphoreWait();
                var publishTask =
                    client.Publish(new SubEntryPublish(publisherId, publishingId,
                        CompressionHelper.Compress(subEntryMessages, compressionType)));
                if (!publishTask.IsCompletedSuccessfully)
                {
                    await publishTask.ConfigureAwait(false);
                }
            }
        }

        private async Task SemaphoreWait()
        {
            if (!semaphore.Wait(0))
            {
                // Nope, we have maxed our In-Flight messages, let's asynchronously wait for confirms
                if (!await semaphore.WaitAsync(1000).ConfigureAwait(false))
                {
                    Console.WriteLine("Semaphore Wait timeout during publishing.");
                }
            }
        }

        public async ValueTask Send(ulong publishingId, Message message)
        {
            await SemaphoreWait();

            var msg = new OutgoingMsg(publisherId, publishingId, message);

            // Let's see if we can write a message to the channel without having to wait
            if (!messageBuffer.Writer.TryWrite(msg))
            {
                // Nope, channel is full and being processed, let's asynchronously wait until we can buffer the message
                await messageBuffer.Writer.WriteAsync(msg).ConfigureAwait(false);
            }
        }

        private async Task ProcessBuffer()
        {
            // TODO: make the batch size configurable.
            var messages = new List<(ulong, Message)>(100);
            while (await messageBuffer.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (messageBuffer.Reader.TryRead(out OutgoingMsg msg))
                {
                    messages.Add((msg.PublishingId, msg.Data));
                    if (messages.Count == 100)
                    {
                        await SendMessages(messages).ConfigureAwait(false);
                    }
                }

                if (messages.Count > 0)
                {
                    await SendMessages(messages).ConfigureAwait(false);
                }
            }

            async Task SendMessages(List<(ulong, Message)> messages)
            {
                var publishTask = client.Publish(new Publish(publisherId, messages));
                if (!publishTask.IsCompletedSuccessfully)
                {
                    await publishTask.ConfigureAwait(false);
                }

                messages.Clear();
            }
        }

        public async Task<ResponseCode> Close()
        {
            if (_disposed)
                return ResponseCode.Ok;

            var deletePublisherResponse = await this.client.DeletePublisher(this.publisherId);
            var result = deletePublisherResponse.ResponseCode;
            var closed = this.client.MaybeClose($"client-close-publisher: {this.publisherId}");
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"client-close-publisher: {this.publisherId}");
            _disposed = true;
            return result;
        }

        public static async Task<Producer> Create(ClientParameters clientParameters,
            ProducerConfig config,
            StreamInfo metaStreamInfo)
        {
            var client = await RoutingHelper<Routing>.LookupLeaderConnection(clientParameters, metaStreamInfo);
            var producer = new Producer((Client) client, config);
            await producer.Init();
            return producer;
        }

        private void Dispose(bool disposing)
        {
            if (!disposing) return;
            if (_disposed) return;

            var closeProducer = this.Close();
            closeProducer.Wait(1000);
            ClientExceptions.MaybeThrowException(closeProducer.Result,
                $"Error during remove producer. Producer: {this.publisherId}");
        }


        public void Dispose()
        {
            try
            {
                Dispose(true);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error during disposing producer: {this.publisherId}, " +
                                  $"error: {e}");
            }

            GC.SuppressFinalize(this);
        }
    }
}