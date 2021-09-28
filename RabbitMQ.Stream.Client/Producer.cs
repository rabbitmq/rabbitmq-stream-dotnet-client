using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Net;
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
        private readonly Dictionary<ulong, Message> pending = new();
        private readonly ConcurrentQueue<TaskCompletionSource> flows = new();

        private Producer(Client client, ProducerConfig config)
        {
            this.client = client;
            this.config = config;
        }

        private async Task Init()
        {
            var (pubId, response) = await client.DeclarePublisher(config.Reference,
                config.Stream,
                publishingIds =>
                {
                    foreach (var id in publishingIds)
                    {
                        var msg = pending[id];
                        pending.Remove(id);
                        config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = ResponseCode.Ok,
                            Message = msg
                        });
                    }

                    if (pending.Count < config.MaxInFlight && !flows.IsEmpty)
                    {
                        if (flows.TryDequeue(out var result))
                        {
                            result.SetResult();
                        }
                    }
                },
                errors =>
                {
                    foreach (var (id, code) in errors)
                    {
                        var msg = pending[id];
                        pending.Remove(id);
                        config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = code,
                            Message = msg
                        });
                        if (pending.Count < config.MaxInFlight)
                        {
                            if (flows.TryDequeue(out var result))
                            {
                                result.SetResult();
                            }
                        }
                    }
                });

            this.publisherId = pubId;
        }

        private bool PublishLimitCheck()
        {
            return pending.Count < config.MaxInFlight;
        }
        
        public async Task Send(ulong publishingId, Message message)
        {
            if (pending.Count >= config.MaxInFlight)
            {
                var tcs = new TaskCompletionSource();
                flows.Enqueue(tcs);
                await tcs.Task;
            }
            
            var _ = client.Publish(new OutgoingMsg(publisherId, publishingId, message));
            pending.Add(publishingId, message);
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