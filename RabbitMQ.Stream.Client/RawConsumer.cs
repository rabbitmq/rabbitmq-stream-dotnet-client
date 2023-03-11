// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
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
        private readonly Channel<Chunk> _chunksBuffer;
        private const int InitialCredit = 2;

        private RawConsumer(Client client, RawConsumerConfig config, ILogger logger = null)
        {
            // _chunksBuffer is a channel that is used to buffer the chunks
            _chunksBuffer = Channel.CreateBounded<Chunk>(new BoundedChannelOptions(InitialCredit)
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait
            });

            _client = client;
            _config = config;
            _logger = logger ?? NullLogger.Instance;

            ProcessChunks();
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
            await _client.StoreOffset(_config.Reference, _config.Stream, offset).ConfigureAwait(false);
        }

        public static async Task<IConsumer> Create(
            ClientParameters clientParameters,
            RawConsumerConfig config,
            StreamInfo metaStreamInfo,
            ILogger logger = null
        )
        {
            var client = await RoutingHelper<Routing>.LookupRandomConnection(clientParameters, metaStreamInfo, logger)
                .ConfigureAwait(false);
            var consumer = new RawConsumer((Client)client, config, logger);
            await consumer.Init().ConfigureAwait(false);
            return consumer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task ParseChunk(Chunk chunk)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            Message MessageFromSequence(ref ReadOnlySequence<byte> unCompressedData, ref int compressOffset)
            {
                try
                {
                    var slice = unCompressedData.Slice(compressOffset, 4);
                    compressOffset += WireFormatting.ReadUInt32(ref slice, out var len);
                    slice = unCompressedData.Slice(compressOffset, len);
                    compressOffset += (int)len;

                    // Here we use the Message.From(ref ReadOnlySequence<byte> seq ..) method to parse the message
                    // instead of the Message From(ref SequenceReader<byte> reader ..) method
                    // Since the ParseChunk is async and we cannot use the ref SequenceReader<byte> reader
                    // See https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/250 for more details

                    var message = Message.From(ref slice, len);
                    return message;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error while parsing message. Message will be skipped. " +
                                        "Please report this issue to the RabbitMQ team on GitHub {Repo}",
                        Consts.RabbitMQClientRepo);
                }

                return null;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            async Task DispatchMessage(Message message, ulong i)
            {
                try
                {
                    message.MessageOffset = chunk.ChunkId + i;
                    if (MaybeDispatch(message.MessageOffset))
                    {
                        if (!Token.IsCancellationRequested)
                        {
                            await _config.MessageHandler(this,
                                new MessageContext(message.MessageOffset,
                                    TimeSpan.FromMilliseconds(chunk.Timestamp)),
                                message).ConfigureAwait(false);
                        }
                    }
                }

                catch (OperationCanceledException)
                {
                    _logger.LogWarning(
                        "OperationCanceledException. The consumer id: {SubscriberId} reference: {Reference} has been closed while consuming messages",
                        _subscriberId, _config.Reference);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error while processing chunk: {ChunkId}", chunk.ChunkId);
                }
            }

            var chunkBuffer = new ReadOnlySequence<byte>(chunk.Data);

            var numRecords = chunk.NumRecords;
            var offset = 0; // it is used to calculate the offset in the chunk.
            ulong messageOffset = 0; // it is used to calculate the message offset. It is the chunkId + messageOffset
            while (numRecords != 0)
            {
                // (entryType & 0x80) == 0 is standard entry
                // (entryType & 0x80) != 0 is compress entry (used for subEntry)
                // In Case of subEntry the entryType is the compression type
                // In case of standard entry the entryType si part of the message
                var slice = chunkBuffer.Slice(offset, 1);
                offset += WireFormatting.ReadByte(ref slice, out var entryType);
                var isSubEntryBatch = (entryType & 0x80) != 0;
                if (isSubEntryBatch)
                {
                    // it means that it is a sub-entry batch 
                    // We continue to read from the stream to decode the subEntryChunk values
                    slice = chunkBuffer.Slice(offset);

                    offset += SubEntryChunk.Read(ref slice, entryType, out var subEntryChunk);
                    var unCompressedData = CompressionHelper.UnCompress(
                        subEntryChunk.CompressionType,
                        new ReadOnlySequence<byte>(subEntryChunk.Data),
                        subEntryChunk.DataLen,
                        subEntryChunk.UnCompressedDataSize);

                    var compressOffset = 0;
                    for (ulong z = 0; z < subEntryChunk.NumRecordsInBatch; z++)
                    {
                        var message = MessageFromSequence(ref unCompressedData, ref compressOffset);
                        await DispatchMessage(message, messageOffset++).ConfigureAwait(false);
                    }

                    numRecords -= subEntryChunk.NumRecordsInBatch;
                }
                else
                {
                    // Ok the entry is a standard entry
                    // we need to rewind the offset to -1 to one byte to decode the messages
                    offset--;
                    var message = MessageFromSequence(ref chunkBuffer, ref offset);
                    await DispatchMessage(message, messageOffset++).ConfigureAwait(false);
                    numRecords--;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessChunks()
        {
            Task.Run(async () =>
            {
                try
                {
                    while (await _chunksBuffer.Reader.WaitToReadAsync(Token).ConfigureAwait(false))
                    {
                        while (_chunksBuffer.Reader.TryRead(out var chunk))
                        {
                            // We send the credit to the server to allow the server to send more messages
                            // we request the credit before process the check to keep the network busy
                            await _client.Credit(_subscriberId, 1).ConfigureAwait(false);
                            await ParseChunk(chunk).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception e)
                {
                    if (Token.IsCancellationRequested)
                        return;
                    _logger.LogError(e, "Error while consuming messages");
                }
            }, Token);
        }

        private async Task Init()
        {
            _config.Validate();

            _client.ConnectionClosed += async reason =>
            {
                await Close().ConfigureAwait(false);
                if (_config.ConnectionClosedHandler != null)
                {
                    await _config.ConnectionClosedHandler(reason).ConfigureAwait(false);
                }
            };
            if (_config.MetadataHandler != null)
            {
                _client.Parameters.MetadataHandler += _config.MetadataHandler;
            }

            var consumerProperties = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(_config.Reference))
            {
                consumerProperties["name"] = _config.Reference;
            }

            if (_config.IsSingleActiveConsumer)
            {
                consumerProperties["single-active-consumer"] = "true";
                if (!string.IsNullOrEmpty(_config.SuperStream))
                {
                    consumerProperties["super-stream"] = _config.SuperStream;
                }
            }

            // this the default value for the consumer.
            _config.StoredOffsetSpec = _config.OffsetSpec;

            var (consumerId, response) = await _client.Subscribe(
                _config,
                InitialCredit,
                consumerProperties,
                async deliver =>
                {
                    // Send the chunk to the _chunksBuffer
                    // in this way the chunks are processed in a separate thread
                    // this wont' block the socket thread
                    // introduced https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/250
                    await _chunksBuffer.Writer.WriteAsync(deliver.Chunk, Token).ConfigureAwait(false);
                }, async b =>
                {
                    if (_config.ConsumerUpdateListener != null)
                    {
                        // in this case the StoredOffsetSpec is overridden by the ConsumerUpdateListener
                        // since the user decided to override the default behavior
                        _config.StoredOffsetSpec = await _config.ConsumerUpdateListener(
                            _config.Reference,
                            _config.Stream,
                            b).ConfigureAwait(false);
                    }

                    return _config.StoredOffsetSpec;
                }
            ).ConfigureAwait(false);
            if (response.ResponseCode == ResponseCode.Ok)
            {
                _subscriberId = consumerId;
                return;
            }

            throw new CreateConsumerException($"consumer could not be created code: {response.ResponseCode}");
        }

        public async Task<ResponseCode> Close()
        {
            // this unlock the consumer if it is waiting for a message
            // see DispatchMessage method where the token is used
            MaybeCancelToken();
            if (_client.IsClosed)
            {
                return ResponseCode.Ok;
            }

            var result = ResponseCode.Ok;
            try
            {
                // The  default timeout is usually 10 seconds 
                // in this case we reduce the waiting time
                // the consumer could be removed because of stream deleted 
                // so it is not necessary to wait.
                var unsubscribeResponse =
                    await _client.Unsubscribe(_subscriberId).WaitAsync(Consts.MidWait).ConfigureAwait(false);
                result = unsubscribeResponse.ResponseCode;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error removing the consumer id: {SubscriberId} from the server", _subscriberId);
            }

            var closed = await _client.MaybeClose($"_client-close-subscriber: {_subscriberId}").ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"_client-close-subscriber: {_subscriberId}");
            _logger.LogDebug("Consumer {SubscriberId} closed", _subscriberId);
            return result;
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

            try
            {
                var closeConsumer = Close();
                if (!closeConsumer.Wait(Consts.ShortWait))
                {
                    Debug.WriteLine($"consumer did not close within {Consts.ShortWait}");
                }

                ClientExceptions.MaybeThrowException(closeConsumer.Result,
                    $"Error during remove producer. Subscriber: {_subscriberId}");
            }
            finally
            {
                _disposed = true;
            }
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
            finally
            {
                GC.SuppressFinalize(this);
            }
        }
    }
}
