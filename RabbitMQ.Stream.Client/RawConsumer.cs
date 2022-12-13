// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client
{
    /// <summary>
    /// MessageContext contains message metadata information
    /// 
    /// </summary>
    public struct MessageContext
    {
        /// <summary>
        /// Message offset inside the log
        /// each single message has its own offset
        /// </summary>
        public ulong Offset { get; }

        /// <summary>
        /// The timestamp of the message chunk.
        /// A chunk (usually) contains multiple messages
        /// The timestamp is the same for all messages in the chunk.
        /// A chunk is simply a batch of messages. 
        /// </summary>
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

    public record RawConsumerConfig : IConsumerConfig
    {
        public RawConsumerConfig(string stream)
        {
            if (string.IsNullOrWhiteSpace(stream))
            {
                throw new ArgumentException("Stream cannot be null or whitespace.", nameof(stream));
            }

            Stream = stream;
        }

        internal void Validate()
        {
            if (IsSingleActiveConsumer && (Reference == null || Reference.Trim() == string.Empty))
            {
                throw new ArgumentException("With single active consumer, the reference must be set.");
            }
        }

        // it is needed to be able to add the subscriptions arguments
        // see consumerProperties["super-stream"] = SuperStream;
        // in this way the consumer is notified is something happens in the super stream
        // it is internal because it is used only internally
        internal string SuperStream { get; set; }

        public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

        // stream name where the consumer will consume the messages.
        // stream must exist before the consumer is created.
        public string Stream { get; }

        public Func<RawConsumer, MessageContext, Message, Task> MessageHandler { get; set; }

        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };
    }

    public class RawConsumer : AbstractEntity, IConsumer, IDisposable
    {
        private bool _disposed;
        private readonly RawConsumerConfig _config;
        private byte _subscriberId;
        private readonly ILogger _logger;

        private RawConsumer(Client client, RawConsumerConfig config, ILogger logger = null)
        {
            _client = client;
            _config = config;
            _logger = logger ?? NullLogger.Instance;
        }

        // if a user specify a custom offset 
        // the _client must filter messages
        // and dispatch only the messages starting from the 
        // user offset.
        private bool MaybeDispatch(ulong offset)
        {
            return _config.StoredOffsetSpec switch
            {
                OffsetTypeOffset offsetTypeOffset =>
                    !(offset < offsetTypeOffset.OffsetValue),
                _ => true
            };
        }

        public async Task StoreOffset(ulong offset)
        {
            await _client.StoreOffset(_config.Reference, _config.Stream, offset);
        }

        public static async Task<IConsumer> Create(
            ClientParameters clientParameters,
            RawConsumerConfig config,
            StreamInfo metaStreamInfo,
            ILogger logger = null
        )
        {
            var client = await RoutingHelper<Routing>.LookupRandomConnection(clientParameters, metaStreamInfo, logger);
            var consumer = new RawConsumer((Client)client, config, logger);
            await consumer.Init();
            return consumer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ParseChunk(Chunk chunk)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void DispatchMessage(ref SequenceReader<byte> sequenceReader, ulong i)
            {
                WireFormatting.ReadUInt32(ref sequenceReader, out var len);
                try
                {
                    var message = Message.From(ref sequenceReader, len);
                    message.MessageOffset = chunk.ChunkId + i;
                    if (MaybeDispatch(message.MessageOffset))
                    {
                        _config.MessageHandler(this,
                            new MessageContext(message.MessageOffset, TimeSpan.FromMilliseconds(chunk.Timestamp)),
                            message).GetAwaiter().GetResult();
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error while processing chunk: {ChunkId}", chunk.ChunkId);
                }
            }

            var reader = new SequenceReader<byte>(chunk.Data);
            if (chunk.HasSubEntries)
            {
                // it means that it is a subentry batch 
                var numRecords = chunk.NumRecords;
                while (numRecords != 0)
                {
                    SubEntryChunk.Read(ref reader, out var subEntryChunk);
                    var unCompressedData = CompressionHelper.UnCompress(
                        subEntryChunk.CompressionType,
                        subEntryChunk.Data,
                        subEntryChunk.DataLen,
                        subEntryChunk.UnCompressedDataSize);
                    var readerUnCompressed = new SequenceReader<byte>(unCompressedData);

                    for (ulong z = 0; z < subEntryChunk.NumRecordsInBatch; z++)
                    {
                        DispatchMessage(ref readerUnCompressed, z);
                    }

                    numRecords -= subEntryChunk.NumRecordsInBatch;
                }
            }
            else
            {
                // Standard chunk. 
                for (ulong i = 0; i < chunk.NumEntries; i++)
                {
                    DispatchMessage(ref reader, i);
                }
            }
        }

        private async Task Init()
        {
            _config.Validate();

            _client.ConnectionClosed += async reason =>
            {
                if (_config.ConnectionClosedHandler != null)
                {
                    await _config.ConnectionClosedHandler(reason);
                }
            };
            if (_config.MetadataHandler != null)
            {
                _client.Parameters.MetadataHandler += _config.MetadataHandler;
            }

            var consumerProperties = new Dictionary<string, string>();
            if (_config.IsSingleActiveConsumer)
            {
                consumerProperties["name"] = _config.Reference;
                consumerProperties["single-active-consumer"] = "true";
                if (!string.IsNullOrEmpty(_config.SuperStream))
                {
                    consumerProperties["super-stream"] = _config.SuperStream;
                }
            }

            // this the default value for the consumer.
            _config.StoredOffsetSpec = _config.OffsetSpec;
            const ushort InitialCredit = 10;

            var (consumerId, response) = await _client.Subscribe(
                _config,
                InitialCredit,
                consumerProperties,
                async deliver =>
                {
                    // receive the chunk from the deliver
                    // before parse the chunk, we ask for more credits
                    // in thi way we keep the network busy
                    await _client.Credit(deliver.SubscriptionId, 1);
                    // parse the chunk, we have another function because the sequence reader
                    // can't be used in async context
                    ParseChunk(deliver.Chunk);
                }, async b =>
                {
                    if (_config.ConsumerUpdateListener != null)
                    {
                        // in this case the StoredOffsetSpec is overridden by the ConsumerUpdateListener
                        // since the user decided to override the default behavior
                        _config.StoredOffsetSpec = await _config.ConsumerUpdateListener(
                            _config.Reference,
                            _config.Stream,
                            b);
                    }

                    return _config.StoredOffsetSpec;
                }
            );
            if (response.ResponseCode == ResponseCode.Ok)
            {
                _subscriberId = consumerId;
                return;
            }

            throw new CreateConsumerException($"consumer could not be created code: {response.ResponseCode}");
        }

        public async Task<ResponseCode> Close()
        {
            if (_client.IsClosed)
            {
                return ResponseCode.Ok;
            }

            var result = ResponseCode.Ok;
            try
            {
                var deleteConsumerResponseTask = _client.Unsubscribe(_subscriberId);
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
                _logger.LogError(e, "Error removing the consumer id: {SubscriberId} from the server", _subscriberId);
            }

            var closed = _client.MaybeClose($"_client-close-subscriber: {_subscriberId}");
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"_client-close-subscriber: {_subscriberId}");
            _disposed = true;
            _logger.LogDebug("Consumer {SubscriberId} closed", _subscriberId);
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
                $"Error during remove producer. Subscriber: {_subscriberId}");
        }

        public void Dispose()
        {
            try
            {
                Dispose(true);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error during disposing of consumer: {SubscriberId}.", _subscriberId);
            }

            GC.SuppressFinalize(this);
        }
    }
}
