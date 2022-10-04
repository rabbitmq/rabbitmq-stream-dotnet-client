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

    internal struct ConsumerEvents
    {
        public ConsumerEvents(Func<Deliver, Task> deliverHandler,
            Func<bool, Task<IOffsetType>> consumerUpdateHandler)
        {
            DeliverHandler = deliverHandler;
            ConsumerUpdateHandler = consumerUpdateHandler;
        }

        public Func<Deliver, Task> DeliverHandler { get; }
        public Func<bool, Task<IOffsetType>> ConsumerUpdateHandler { get; }
    }

    public record ConsumerConfig : INamedEntity
    {
        // StoredOffsetSpec configuration it is needed to keep the offset spec.
        // since the offset can be decided from the ConsumerConfig.OffsetSpec.
        // and from ConsumerConfig.ConsumerUpdateListener.
        // needed also See also consumer:MaybeDispatch/1.
        // It is not public because it is not needed for the user.
        internal IOffsetType StoredOffsetSpec { get; set; }

        internal void Validate()
        {
            if (IsSingleActiveConsumer && (Reference == null || Reference.Trim() == string.Empty))
            {
                throw new ArgumentException("With single active consumer, the reference must be set.");
            }
        }

        // stream name where the consumer will consume the messages.
        // stream must exist before the consumer is created.
        public string Stream { get; set; }
        public string Reference { get; set; }
        public Func<Consumer, MessageContext, Message, Task> MessageHandler { get; set; }
        public Func<string, Task> ConnectionClosedHandler { get; set; }

        public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

        // ClientProvidedName is used to identify TCP connection name.
        public string ClientProvidedName { get; set; } = "dotnet-stream-consumer";

        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };

        // SingleActiveConsumer is used to indicate that there is only one consumer active for the stream.
        // given a consumer reference. 
        // Consumer Reference can't be null or Empty.

        public bool IsSingleActiveConsumer { get; set; } = false;

        // config.ConsumerUpdateListener is the callback for when the consumer is updated due
        // to single active consumer. 
        // return IOffsetType to indicate the offset to be used for the next consumption.
        // if the ConsumerUpdateListener==null the OffsetSpec will be used.
        public Func<string, string, bool, Task<IOffsetType>> ConsumerUpdateListener { get; set; } = null;
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
            return config.StoredOffsetSpec switch
            {
                OffsetTypeOffset offsetTypeOffset =>
                    !(offset < offsetTypeOffset.OffsetValue),
                _ => true
            };
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
            config.Validate();

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

            var consumerProperties = new Dictionary<string, string>();
            if (config.IsSingleActiveConsumer)
            {
                consumerProperties["name"] = config.Reference;
                consumerProperties["single-active-consumer"] = "true";
            }

            // this the default value for the consumer.
            config.StoredOffsetSpec = config.OffsetSpec;
            const ushort InitialCredit = 2;

            var (consumerId, response) = await client.Subscribe(
                config,
                InitialCredit,
                consumerProperties,
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
                }, async b =>
                {
                    if (config.ConsumerUpdateListener != null)
                    {
                        // in this case the StoredOffsetSpec is overridden by the ConsumerUpdateListener
                        // since the user decided to override the default behavior
                        config.StoredOffsetSpec = await config.ConsumerUpdateListener(
                            config.Reference,
                            config.Stream,
                            b);
                    }

                    return config.StoredOffsetSpec;
                }
            );
            if (response.ResponseCode == ResponseCode.Ok)
            {
                subscriberId = consumerId;
                return;
            }

            throw new CreateConsumerException($"consumer could not be created code: {response.ResponseCode}");
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
                var deleteConsumerResponseTask = client.Unsubscribe(subscriberId);
                // The  default timeout is usually 10 seconds 
                // in this case we reduce the waiting time
                // the consumer could be removed because of stream deleted 
                // so it is not necessary to wait.
                await deleteConsumerResponseTask.WaitAsync(TimeSpan.FromSeconds(3));
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
