using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{

    public struct MessageContext
    {
        public ulong Offset { get; }

        public TimeSpan Timestamp { get; }

        public MessageContext(ulong offset, TimeSpan timestamp)
        {
            this.Offset = offset;
            this.Timestamp = timestamp;
        }
    }
    
    public record ConsumerConfig
    {
        public string Stream { get; init; }
        public string Reference { get; init; }
        public Func<Consumer, MessageContext, Message, Task> MessageHandler { get; init; }
        public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();
    }
    
    public class Consumer
    {
        private readonly Client client;
        private readonly ConsumerConfig config;

        private Consumer(Client client, ConsumerConfig config)
        {
            this.client = client;
            this.config = config;
        }

        public void Commit(ulong offset)
        {
        }

        public static async Task<Consumer> Create(ClientParameters clientParameters, ConsumerConfig config)
        {
            var client = await Client.Create(clientParameters);
            var consumer = new Consumer(client, config);
            await consumer.Init();
            return consumer;
        }

        private async Task Init()
        {
            var (consumerId, response) = await client.Subscribe(
                config.Stream,
                config.OffsetSpec, 2,
                new Dictionary<string, string>(),
                async deliver =>
                {
                    foreach (var messageEntry in deliver.Messages)
                    {
                        var message = Message.From(messageEntry.Data); //data should be AMQP 1.0 encoded
                        await config.MessageHandler(this,
                            new MessageContext(messageEntry.Offset,
                                TimeSpan.FromMilliseconds(deliver.Chunk.Timestamp)),
                            message);
                    }

                    return;

                });
            if (response.Code == ResponseCode.Ok)
                return;
            throw new CreateConsumerException($"consumer could not be created code: {response.Code}");
        }
    }
}