using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Net;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public struct Confirmation
    {
        public ResponseCode Code { get; set; }
        public Message Message { get; set; }
    }

    public record ProducerConfig
    {
        public string Stream { get; init; }
        public string Reference { get; init; }
        public int MaxInFlight { get; set; } = 1000;
        public Action<Confirmation> ConfirmHandler { get; set; } = _ => { };
    }

    public class Producer
    {
        private readonly Client client;
        private byte publisherId;
        private readonly ProducerConfig config;
        private readonly Dictionary<ulong, Message> pending = new Dictionary<ulong, Message>();

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
                        config.ConfirmHandler(new Confirmation
                        {
                            Code = ResponseCode.Ok,
                            Message = msg
                        });
                    }
                },
                errors =>
                {
                    foreach (var (id, code) in errors)
                    {
                        var msg = pending[id];
                        config.ConfirmHandler(new Confirmation
                        {
                            Code = code,
                            Message = msg
                        });
                    }
                });

            this.publisherId = pubId;
        }

        public async Task Send(ulong publishingId, Message message)
        {
            // TODO: replace this with a bounds check + wait
            await Task.CompletedTask;
            //await something or other
            var _ = client.Publish(new OutgoingMsg(publisherId, publishingId, message.Serialize()));
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