// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public struct Confirmation
    {
        public ulong PublishingId { get; set; }
        public ResponseCode Code { get; set; }

        public string Stream { get; set; }
    }

    public record RawProducerConfig : IProducerConfig
    {
        public string Stream { get; }
        public Func<string, Task> ConnectionClosedHandler { get; set; }
        public Action<Confirmation> ConfirmHandler { get; set; } = _ => { };

        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };

        public RawProducerConfig(string stream)
        {
            if (string.IsNullOrWhiteSpace(stream))
            {
                throw new ArgumentException("Stream cannot be null or whitespace.", nameof(stream));
            }

            Stream = stream;
        }
    }

    public class RawProducer : AbstractEntity, IProducer, IDisposable
    {
        private bool _disposed;
        private byte publisherId;
        private readonly RawProducerConfig config;
        private readonly Channel<OutgoingMsg> messageBuffer;
        private readonly SemaphoreSlim semaphore;

        public int PendingCount => config.MaxInFlight - semaphore.CurrentCount;

        private RawProducer(Client client, RawProducerConfig config)
        {
            this.client = client;
            this.config = config;
            messageBuffer = Channel.CreateBounded<OutgoingMsg>(new BoundedChannelOptions(10000)
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
            Task.Run(ProcessBuffer);
            semaphore = new(config.MaxInFlight, config.MaxInFlight);
        }

        public int MessagesSent => client.MessagesSent;
        public int ConfirmFrames => client.ConfirmFrames;
        public int IncomingFrames => client.IncomingFrames;
        public int PublishCommandsSent => client.PublishCommandsSent;

        private async Task Init()
        {
            client.ConnectionClosed += async reason =>
            {
                if (config.ConnectionClosedHandler != null)
                {
                    await config.ConnectionClosedHandler(reason);
                }
            };

            if (config.MetadataHandler != null)
            {
                client.Parameters.MetadataHandler += config.MetadataHandler;
            }

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
                            Stream = config.Stream
                        });
                    }

                    semaphore.Release(publishingIds.Length);
                },
                errors =>
                {
                    foreach (var (id, code) in errors)
                    {
                        config.ConfirmHandler(new Confirmation { PublishingId = id, Code = code, });
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

        public async ValueTask Send(List<(ulong, Message)> messages)
        {
            PreValidateBatch(messages);
            await InternalBatchSend(messages);
        }

        internal async Task InternalBatchSend(List<(ulong, Message)> messages)
        {
            for (var i = 0; i < messages.Count; i++)
            {
                await SemaphoreWait();
            }

            if (messages.Count != 0 && !client.IsClosed)
            {
                await SendMessages(messages, false).ConfigureAwait(false);
            }
        }

        internal void PreValidateBatch(List<(ulong, Message)> messages)
        {
            if (messages.Count > config.MaxInFlight)
            {
                throw new InvalidOperationException($"Too many messages in batch. " +
                                                    $"Max allowed is {config.MaxInFlight}");
            }

            var totalSize = messages.Sum(message => message.Item2.Size);

            if (totalSize > client.MaxFrameSize)
            {
                throw new InvalidOperationException($"Total size of messages in batch is too big. " +
                                                    $"Max allowed is {client.MaxFrameSize}");
            }
        }

        private async Task SemaphoreWait()
        {
            if (!await semaphore.WaitAsync(0) && !client.IsClosed)
            {
                // Nope, we have maxed our In-Flight messages, let's asynchronously wait for confirms
                if (!await semaphore.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false))
                {
                    LogEventSource.Log.LogWarning("Semaphore Wait timeout during publishing.");
                }
            }
        }

        private async Task SendMessages(List<(ulong, Message)> messages, bool clearMessagesList = true)
        {
            var publishTask = client.Publish(new Publish(publisherId, messages));
            if (!publishTask.IsCompletedSuccessfully)
            {
                await publishTask.ConfigureAwait(false);
            }

            if (clearMessagesList)
            {
                messages.Clear();
            }
        }

        /// <summary>
        /// GetLastPublishingId 
        /// </summary>
        /// <returns>The last sequence id stored by the producer.</returns>
        public async Task<ulong> GetLastPublishingId()
        {
            var response = await client.QueryPublisherSequence(config.Reference, config.Stream);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"GetLastPublishingId stream: {config.Stream}, reference: {config.Reference}");
            return response.Sequence;
        }

        public bool IsOpen()
        {
            return !_disposed && !client.IsClosed;
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
            var messages = new List<(ulong, Message)>(config.MessagesBufferSize);
            while (await messageBuffer.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (messageBuffer.Reader.TryRead(out var msg))
                {
                    messages.Add((msg.PublishingId, msg.Data));
                    if (messages.Count == config.MessagesBufferSize)
                    {
                        await SendMessages(messages).ConfigureAwait(false);
                    }
                }

                if (messages.Count > 0)
                {
                    await SendMessages(messages).ConfigureAwait(false);
                }
            }
        }

        public async Task<ResponseCode> Close()
        {
            if (client.IsClosed)
            {
                return ResponseCode.Ok;
            }

            var result = ResponseCode.Ok;
            try
            {
                var deletePublisherResponseTask = client.DeletePublisher(publisherId);
                // The  default timeout is usually 10 seconds 
                // in this case we reduce the waiting time
                // the producer could be removed because of stream deleted 
                // so it is not necessary to wait.
                await deletePublisherResponseTask.WaitAsync(TimeSpan.FromSeconds(3));
                if (deletePublisherResponseTask.IsCompletedSuccessfully)
                {
                    result = deletePublisherResponseTask.Result.ResponseCode;
                }
            }
            catch (Exception e)
            {
                LogEventSource.Log.LogError($"Error removing the producer id: {publisherId} from the server. {e}");
            }

            var closed = client.MaybeClose($"client-close-publisher: {publisherId}");
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"client-close-publisher: {publisherId}");
            return result;
        }

        public static async Task<IProducer> Create(ClientParameters clientParameters,
            RawProducerConfig config,
            StreamInfo metaStreamInfo)
        {
            var client = await RoutingHelper<Routing>.LookupLeaderConnection(clientParameters, metaStreamInfo);
            var producer = new RawProducer((Client)client, config);
            await producer.Init();
            return producer;
        }

        private void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (_disposed)
            {
                return;
            }

            var closeProducer = Close();
            closeProducer.Wait(TimeSpan.FromSeconds(1));
            ClientExceptions.MaybeThrowException(closeProducer.Result,
                $"Error during remove producer. Producer: {publisherId}");
            _disposed = true;
        }

        public void Dispose()
        {
            try
            {
                Dispose(true);
            }
            catch (Exception e)
            {
                LogEventSource.Log.LogError($"Error during disposing Consumer: {publisherId}.", e);
            }

            GC.SuppressFinalize(this);
        }
    }
}
