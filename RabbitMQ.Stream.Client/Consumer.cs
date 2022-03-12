// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
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
            Offset = offset;
            Timestamp = timestamp;
        }
    }

    public record ConsumerConfig : INamedEntity
    {
        public string Stream { get; set; }
        public string Reference { get; set; }
        public Func<Consumer, MessageContext, Message, Task> MessageHandler { get; set; }
        public Func<string, Task> ConnectionClosedHandler { get; set; }
        public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();
        public string ClientProvidedName { get; set; } = "dotnet-stream-consumer";
    }

    public class Consumer : AbstractEntity, IDisposable
    {
        private bool _disposed;
        private readonly ConsumerConfig config;
        private byte subscriberId;

        private Consumer(Client client, ConsumerConfig config)
        {
            this.client = client;
            this.config = config;
        }

        public async Task StoreOffset(ulong offset)
        {
            await client.StoreOffset(config.Reference, config.Stream, offset);
        }

        public static async Task<Consumer> Create(ClientParameters clientParameters,
            ConsumerConfig config,
            StreamInfo metaStreamInfo)
        {
            var client = await RoutingHelper<Routing>.LookupRandomConnection(clientParameters, metaStreamInfo);
            var consumer = new Consumer((Client)client, config);
            await consumer.Init();
            return consumer;
        }

        private async Task Init()
        {
            client.ConnectionClosed += async (reason) =>
            {
                if (config.ConnectionClosedHandler != null)
                {
                    await config.ConnectionClosedHandler(reason);
                }
            };

            ushort initialCredit = 2;
            var (consumerId, response) = await client.Subscribe(
                config.Stream,
                config.OffsetSpec, initialCredit,
                new Dictionary<string, string>(),
                async deliver =>
                {
                    foreach (var messageEntry in deliver.Messages)
                    {
                        if (config.OffsetSpec is OffsetTypeOffset offset && messageEntry.Offset < offset.OffsetValue)
                        {
                            continue;
                        }

                        var message = Message.From(messageEntry.Data); //data should be AMQP 1.0 encoded
                        await config.MessageHandler(this,
                            new MessageContext(messageEntry.Offset,
                                TimeSpan.FromMilliseconds(deliver.Chunk.Timestamp)),
                            message);
                    }

                    // give one credit after each chunk
                    await client.Credit(deliver.SubscriptionId, 1);
                });
            if (response.ResponseCode == ResponseCode.Ok)
            {
                subscriberId = consumerId;
                return;
            }

            throw new CreateConsumerException($"consumer could not be created code: {response.ResponseCode}");
        }

        public async Task<ResponseCode> Close()
        {
            if (_disposed)
            {
                return ResponseCode.Ok;
            }

            var deleteConsumerResponse = await client.Unsubscribe(subscriberId);
            var result = deleteConsumerResponse.ResponseCode;
            var closed = client.MaybeClose($"client-close-subscriber: {subscriberId}");
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"client-close-subscriber: {subscriberId}");
            _disposed = true;
            return result;
        }

        //
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

            var closeConsumer = Close();
            closeConsumer.Wait(1000);
            ClientExceptions.MaybeThrowException(closeConsumer.Result,
                $"Error during remove producer. Subscriber: {subscriberId}");
        }

        public void Dispose()
        {
            try
            {
                Dispose(true);
            }
            catch (Exception e)
            {
                LogEventSource.Log.LogError($"Error during disposing Consumer: {subscriberId}.", e);
            }

            GC.SuppressFinalize(this);
        }
    }
}
