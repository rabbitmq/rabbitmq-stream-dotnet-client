using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public struct Confirmation
    {
        public ulong PublishingId { get; set; }
        public ResponseCode Code { get; set; }
        public Message Message { get; set; }
    }

    public record ProducerConfig
    {
        public string Stream { get; set; }
        public string Reference { get; set; }
        public int MaxInFlight { get; set; } = 1000;
        public Action<Confirmation> ConfirmHandler { get; set; } = _ => { };
    }

    public class Producer
    {
        private readonly Client client;
        private byte publisherId;
        private readonly ProducerConfig config;
        private readonly ConcurrentDictionary<ulong, Message> pending;
        private readonly Channel<OutgoingMsg> messageBuffer;
        private readonly SemaphoreSlim semaphore = new(0);

        public int PendingCount => pending.Count;

        private readonly ConcurrentQueue<TaskCompletionSource> flows = new();
        private Task publishTask;

        private Producer(Client client, ProducerConfig config)
        {
            this.client = client;
            this.config = config;
            this.pending = new ConcurrentDictionary<ulong, Message>(Environment.ProcessorCount, config.MaxInFlight);
            this.messageBuffer = Channel.CreateBounded<OutgoingMsg>(new BoundedChannelOptions(10000) { AllowSynchronousContinuations = false, SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait });
            this.publishTask = Task.Run(ProcessBuffer);
        }

        public Client Client => client;

        private async Task Init()
        {
            var (pubId, response) = await client.DeclarePublisher(
                config.Reference,
                config.Stream,
                publishingIds =>
                {
                    foreach (var id in publishingIds.Span)
                    {
                        if (pending.TryRemove(id, out Message msg))
                        {
                            config.ConfirmHandler(new Confirmation
                            {
                                PublishingId = id,
                                Code = ResponseCode.Ok,
                                Message = msg
                            });
                        }
                    }

                    if (pending.Count < config.MaxInFlight)
                    {
                        semaphore.Release();
                    }
                },
                errors =>
                {
                    foreach ((ulong id, ResponseCode code) in errors)
                    {
                        if (pending.TryRemove(id, out Message msg))
                        {
                            config.ConfirmHandler(new Confirmation
                            {
                                PublishingId = id,
                                Code = code,
                                Message = msg
                            });
                        }
                    }

                    if (pending.Count < config.MaxInFlight)
                    {
                        semaphore.Release();
                    }
                });

            this.publisherId = pubId;
        }

        public async ValueTask Send(ulong publishingId, Message message)
        {
            if (pending.Count >= config.MaxInFlight)
            {
                if (!await semaphore.WaitAsync(1000))
                {
                    Console.WriteLine("SEMAPHORE TIMEOUT");
                }
            }

            var msg = new OutgoingMsg(publisherId, publishingId, message);
            if(!messageBuffer.Writer.TryWrite(msg))
            { 
                await messageBuffer.Writer.WriteAsync(msg).ConfigureAwait(false);
            }

            pending.TryAdd(publishingId, message);
        }

        public async Task ProcessBuffer()
        {
            var messages = new List<(ulong, Message)>(100);
            while (await messageBuffer.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (messageBuffer.Reader.TryRead(out OutgoingMsg msg))
                {
                    messages.Add((msg.PublishingId, msg.Data));
                    if (messages.Count == 1000)
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

        public static async Task<Producer> Create(ClientParameters clientParameters, ProducerConfig config)
        {
            var client = await Client.Create(clientParameters);
            var producer = new Producer(client, config);
            await producer.Init();
            return producer;
        }
    }
}