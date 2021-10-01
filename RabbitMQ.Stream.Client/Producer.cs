using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
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
        private int pendingCount = 0;

        public int PendingCount => pendingCount;

        private readonly ConcurrentQueue<TaskCompletionSource> flows = new();
        private readonly SemaphoreSlim semaphore = new(0);

        private Producer(Client client, ProducerConfig config)
        {
            this.client = client;
            this.config = config;
            this.pending = new ConcurrentDictionary<ulong, Message>(Environment.ProcessorCount, config.MaxInFlight);
        }

        public Client Client => client;

        private async Task Init()
        {
            var (pubId, response) = await client.DeclarePublisher(
                config.Reference,
                config.Stream,
                publishingIds =>
                {
                    foreach (var id in publishingIds)
                    {
                        pending.Remove(id, out var msg);
                        config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = ResponseCode.Ok,
                            Message = msg
                        });
                    }

                    if (pending.Count < config.MaxInFlight)
                    {
                        semaphore.Release();
                    }
                },
                errors =>
                {
                    foreach (var (id, code) in errors)
                    {
                        pending.Remove(id, out var msg);
                        config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = code,
                            Message = msg
                        });
                    }

                    if (pending.Count < config.MaxInFlight)
                    {
                        if (flows.TryDequeue(out var result))
                        {
                            result.SetResult();
                        }
                    }
                });

            this.publisherId = pubId;
        }

        public async Task Send(ulong publishingId, Message message)
        {
            pendingCount = pending.Count;
            if (pendingCount >= config.MaxInFlight)
            {
                await semaphore.WaitAsync();
            }
            
            var _ = client.Publish(publisherId, publishingId, message);
            pending.TryAdd(publishingId, message);
            //Debug.Assert(added);
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