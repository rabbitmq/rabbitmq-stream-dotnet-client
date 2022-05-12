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

        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };
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

        // if a user specify a custom offset 
        // the client must filter messages
        // and dispatch only the messages starting from the 
        // user offset.
        private bool MaybeDispatch(ulong offset)
        {
            return !(config.OffsetSpec is OffsetTypeOffset offsetSpec
                     && offset < offsetSpec.OffsetValue);
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

            ushort initialCredit = 2;
            var (consumerId, response) = await client.Subscribe(
                config.Stream,
                config.OffsetSpec, initialCredit,
                new Dictionary<string, string>(),
                async deliver =>
                {
                    foreach (var messageEntry in deliver.Messages)
                    {
                        if (!MaybeDispatch(messageEntry.Offset))
                        {
                            continue;
                        }

                        var message = Message.From(messageEntry.Data);
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

        public Task<ResponseCode> Close()
        {
            if (client.IsClosed)
            {
                return Task.FromResult(ResponseCode.Ok);
            }

            var result = ResponseCode.Ok;
            try
            {
                var deleteConsumerResponseTask = client.Unsubscribe(subscriberId);
                // The  default timeout is usually 10 seconds 
                // in this case we reduce the waiting time
                // the consumer could be removed because of stream deleted 
                // so it is not necessary to wait.
                deleteConsumerResponseTask.Wait(TimeSpan.FromSeconds(3));
                if (deleteConsumerResponseTask.IsCompletedSuccessfully)
                {
                    result = deleteConsumerResponseTask.Result.ResponseCode;
                }
            }
            catch (Exception e)
            {
                LogEventSource.Log.LogError($"Error removing the consumer id: {subscriberId} from the server. {e}");
            }

            var closed = client.MaybeClose($"client-close-subscriber: {subscriberId}");
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"client-close-subscriber: {subscriberId}");
            _disposed = true;
            return Task.FromResult(result);
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
            closeConsumer.Wait(TimeSpan.FromSeconds(1));
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
