// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
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

        /// <summary>
        /// The number of messages in the current chunk
        /// </summary>
        public uint ChunkMessagesCount { get; }

        /// <summary>
        /// It is the chunk id that can help to understand the ChuckMessagesCount
        /// </summary>
        public ulong ChunkId { get; }

        public MessageContext(ulong offset, TimeSpan timestamp, uint chunkMessagesCount, ulong chunkId)
        {
            Offset = offset;
            Timestamp = timestamp;
            ChunkMessagesCount = chunkMessagesCount;
            ChunkId = chunkId;
        }
    }

    public struct ConsumerEvents
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

    public record ConsumerFilter
    {
        public List<string> Values { get; set; }
        public bool MatchUnfiltered { get; set; }
        public Func<Message, bool> PostFilter { get; set; }
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

            if (IsFiltering && !AvailableFeaturesSingleton.Instance.PublishFilter)
            {
                throw new UnsupportedOperationException(Consts.FilterNotSupported);
            }

            switch (ConsumerFilter)
            {
                case { PostFilter: null }:
                    throw new ArgumentException("PostFilter must be provided when Filter is set");
                case { Values.Count: 0 }:
                    throw new ArgumentException("Values must be provided when Filter is set");
            }
        }

        internal bool IsFiltering => ConsumerFilter is { Values.Count: > 0 };

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

        public Func<string, Task> ConnectionClosedHandler { get; set; }
    }

    public class RawConsumer : AbstractEntity, IConsumer, IDisposable
    {
        private readonly RawConsumerConfig _config;

        private readonly Channel<(Chunk, ChunkAction)> _chunksBuffer;

        private readonly ushort _initialCredits;

        // _completeSubscription is used to notify the ProcessChunks task
        // that the subscription is completed and so it can start to process the chunks
        // this is needed because the socket starts to receive the chunks before the subscription_id is 
        // assigned. 
        private readonly TaskCompletionSource _completeSubscription =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected sealed override string DumpEntityConfiguration()
        {
            var superStream = string.IsNullOrEmpty(_config.SuperStream)
                ? "No SuperStream"
                : $"SuperStream {_config.SuperStream}";
            return
                $"Consumer id {EntityId} for stream: {_config.Stream}, " +
                $"identifier: {_config.Identifier}, " +
                $"reference: {_config.Reference}, OffsetSpec {_config.OffsetSpec} " +
                $"Client ProvidedName {_config.ClientProvidedName}, " +
                $"{superStream}, IsSingleActiveConsumer: {_config.IsSingleActiveConsumer}, " +
                $"Token IsCancellationRequested: {Token.IsCancellationRequested} ";
        }

        private RawConsumer(Client client, RawConsumerConfig config, ILogger logger = null)
        {
            Logger = logger ?? NullLogger.Instance;
            _initialCredits = config.InitialCredits;
            _config = config;
            Logger.LogDebug("Creating... {DumpEntityConfiguration}", DumpEntityConfiguration());
            Info = new ConsumerInfo(_config.Stream, _config.Reference, _config.Identifier, null);
            // _chunksBuffer is a channel that is used to buffer the chunks

            _chunksBuffer = Channel.CreateBounded<(Chunk, ChunkAction)>(new BoundedChannelOptions(_initialCredits)
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            IsPromotedAsActive = true;
            _client = client;

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

        protected override string GetStream()
        {
            return _config.Stream;
        }

        public async Task StoreOffset(ulong offset)
        {
            await _client.StoreOffset(_config.Reference, _config.Stream, offset).ConfigureAwait(false);
        }

        ////// *********************
        // IsPromotedAsActive is needed to understand if the consumer is active or not
        // by default is active
        // in case of single active consumer can be not active
        // it is important to skip the messages in the chunk that 
        // it is in progress. In this way the promotion will be faster
        // avoiding to block the consumer handler if the user put some
        // long task
        private bool IsPromotedAsActive { get; set; }

        // PromotionLock avoids race conditions when the consumer is promoted as active
        // and the messages are dispatched in parallel.
        // The consumer can be promoted as active with the function ConsumerUpdateListener
        // It is needed when the consumer is single active consumer
        private SemaphoreSlim PromotionLock { get; } = new(1);

        /// <summary>
        /// MaybeLockDispatch locks the dispatch of the messages
        /// it is needed only when the consumer is single active consumer
        /// MaybeLockDispatch is an optimization to avoid to lock the dispatch
        /// when the consumer is not single active consumer
        /// </summary>
        private async Task MaybeLockDispatch()
        {
            if (_config.IsSingleActiveConsumer)
                await PromotionLock.WaitAsync(Token).ConfigureAwait(false);
        }

        /// <summary>
        /// MaybeReleaseLock releases the lock on the dispatch of the messages
        /// Following the MaybeLockDispatch method
        /// </summary>
        private void MaybeReleaseLock()
        {
            if (_config.IsSingleActiveConsumer)
                PromotionLock.Release();
        }

        ////// ********************* 
        public static async Task<IConsumer> Create(
            ClientParameters clientParameters,
            RawConsumerConfig config,
            StreamInfo metaStreamInfo,
            ILogger logger = null
        )
        {
            var client = await RoutingHelper<Routing>
                .LookupLeaderOrRandomReplicasConnection(clientParameters, metaStreamInfo, config.Pool, logger)
                .ConfigureAwait(false);
            var consumer = new RawConsumer((Client)client, config, logger);
            await consumer.Init().ConfigureAwait(false);
            return consumer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task ParseChunk(Chunk chunk)
        {
            try
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                Message MessageFromSequence(ref ReadOnlySequence<byte> unCompressedData, ref int compressOffset)
                {
                    try
                    {
                        var slice = unCompressedData.Slice(compressOffset, 4);
                        compressOffset += WireFormatting.ReadUInt32(ref slice, out var len);
                        Debug.Assert(len > 0);
                        var sliceMsg = unCompressedData.Slice(compressOffset, len);
                        Debug.Assert(sliceMsg.Length == len);
                        compressOffset += (int)len;

                        // Here we use the Message.From(ref ReadOnlySequence<byte> seq ..) method to parse the message
                        // instead of the Message From(ref SequenceReader<byte> reader ..) method
                        // Since the ParseChunk is async and we cannot use the ref SequenceReader<byte> reader
                        // See https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/250 for more details

                        var message = Message.From(ref sliceMsg, len);
                        return message;
                    }
                    catch (Exception e)
                    {
                        if (Token.IsCancellationRequested)
                        {
                            Logger?.LogDebug(
                                "Error while parsing message {ConsumerInfo}, Cancellation Requested, the consumer is closing. ",
                                DumpEntityConfiguration());
                            return null;
                        }

                        Logger?.LogError(e,
                            "Error while parsing message {EntityInfo}.  The message will be skipped. " +
                            "Please report this issue to the RabbitMQ team on GitHub {Repo}",
                            DumpEntityConfiguration(), Consts.RabbitMQClientRepo);
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
                                // it is usually active
                                // it is useful only in single active consumer
                                if (IsPromotedAsActive)
                                {
                                    if (_status != EntityStatus.Open)
                                    {
                                        Logger?.LogDebug(
                                            "{EntityInfo} is not active. message won't dispatched",
                                            DumpEntityConfiguration());
                                    }

                                    // can dispatch only if the consumer is active
                                    // it usually at this point the consumer is active
                                    // Given the way how the ids are generated it is very rare to have the same ids
                                    // it is just a safety check. 
                                    // If the consumer is not open we can just skip the messages
                                    var canDispatch = _status == EntityStatus.Open;

                                    if (_config.IsFiltering)
                                    {
                                        try
                                        {
                                            // post filter is defined by the user
                                            // and can go in error for several reasons
                                            // here we decided to catch the exception and
                                            // log it. The message won't be dispatched
                                            canDispatch = _config.ConsumerFilter.PostFilter(message);
                                        }
                                        catch (Exception e)
                                        {
                                            Logger?.LogError(e,
                                                "Error while filtering message. Message  with offset {MessageOffset} won't be dispatched."
                                                + "Suggestion: review the PostFilter value function"
                                                + "{EntityInfo}",
                                                message.MessageOffset, DumpEntityConfiguration());
                                            canDispatch = false;
                                        }
                                    }

                                    if (canDispatch)
                                    {
                                        await _config.MessageHandler(this,
                                            new MessageContext(message.MessageOffset,
                                                TimeSpan.FromMilliseconds(chunk.Timestamp),
                                                chunk.NumRecords, chunk.ChunkId),
                                            message).ConfigureAwait(false);
                                    }
                                }
                                else
                                {
                                    Logger?.LogDebug(
                                        "{EntityInfo} is not active. message won't dispatched",
                                        DumpEntityConfiguration());
                                }
                            }
                        }
                    }

                    catch (OperationCanceledException)
                    {
                        Logger?.LogWarning(
                            "OperationCanceledException. {EntityInfo} has been closed while consuming messages",
                            DumpEntityConfiguration());
                    }
                    catch (Exception e)
                    {
                        if (Token.IsCancellationRequested)
                        {
                            Logger?.LogDebug(
                                "Dispatching  {EntityInfo}, Cancellation Requested, the consumer is closing. ",
                                DumpEntityConfiguration());
                            return;
                        }

                        Logger?.LogError(e,
                            "Error while Dispatching message, ChunkId : {ChunkId} {EntityInfo}",
                            chunk.ChunkId, DumpEntityConfiguration());
                    }
                }

                var chunkBuffer = new ReadOnlySequence<byte>(chunk.Data);

                Debug.Assert(chunkBuffer.Length == chunk.Data.Length);

                var numRecords = chunk.NumRecords;
                var offset = 0; // it is used to calculate the offset in the chunk.
                ulong
                    messageOffset = 0; // it is used to calculate the message offset. It is the chunkId + messageOffset
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
                            await MaybeLockDispatch().ConfigureAwait(false);
                            try
                            {
                                await DispatchMessage(message, messageOffset++).ConfigureAwait(false);
                            }
                            finally
                            {
                                MaybeReleaseLock();
                            }
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
            catch (Exception e)
            {
                Logger?.LogError(e,
                    "Error while parsing chunk, ChunkId : {ChunkId} {EntityInfo}",
                    chunk.ChunkId, DumpEntityConfiguration());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessChunks()
        {
            Task.Run(async () =>
            {
                // need to wait the subscription is completed 
                // else the _subscriberId could be incorrect
                _completeSubscription.Task.Wait();

                try
                {
                    while (!Token.IsCancellationRequested &&
                           await _chunksBuffer.Reader.WaitToReadAsync(Token).ConfigureAwait(false)) // 
                    {
                        while (_chunksBuffer.Reader.TryRead(out var chunkWithAction))
                        {
                            // We send the credit to the server to allow the server to send more messages
                            // we request the credit before process the check to keep the network busy

                            var (chunk, action) = chunkWithAction;
                            try
                            {
                                if (Token.IsCancellationRequested)
                                    break;
                                await _client.Credit(EntityId, 1).ConfigureAwait(false);
                            }
                            catch (InvalidOperationException)
                            {
                                // The client has been closed
                                // Suppose a scenario where the client is closed and the ProcessChunks task is still running
                                // we remove the the subscriber from the client and we close the client
                                // The ProcessChunks task will try to send the credit to the server
                                // The client will throw an InvalidOperationException
                                // since the connection is closed
                                // In this case we don't want to log the error to avoid log noise
                                Logger?.LogDebug(
                                    "Can't send the credit {EntityInfo}: The TCP client has been closed",
                                    DumpEntityConfiguration());
                                break;
                            }

                            // We need to check the cancellation token status because the exception can be thrown
                            // because the cancellation token has been cancelled
                            // the consumer could take time to handle a single 
                            // this check is a bit redundant but it is useful to avoid to process the chunk
                            // and close the task
                            if (Token.IsCancellationRequested)
                                break;
                            switch (action)
                            {
                                case ChunkAction.Skip:
                                    Logger?.LogDebug(
                                        "The chunk {ChunkId} will be skipped for {EntityInfo}",
                                        chunk.ChunkId, DumpEntityConfiguration());
                                    continue; // skip the chunk
                                case ChunkAction.TryToProcess:
                                    // continue to process the chunk
                                    await ParseChunk(chunk).ConfigureAwait(false);
                                    break;
                            }
                        }
                    }

                    Logger?.LogDebug(
                        "The ProcessChunks {EntityInfo} task has been closed normally",
                        DumpEntityConfiguration());
                }
                catch (Exception e)
                {
                    // We need to check the cancellation token status because the exception can be thrown
                    // because the cancellation token has been cancelled
                    // In this case we don't want to log the error
                    if (Token.IsCancellationRequested)
                    {
                        Logger?.LogDebug(
                            "The ProcessChunks task for the stream: {EntityInfo}  has been closed due to cancellation",
                            DumpEntityConfiguration());
                        return;
                    }

                    Logger?.LogError(e,
                        "Error while process chunks the stream: {EntityInfo} The ProcessChunks task will be closed",
                        DumpEntityConfiguration());
                }
            }, Token);
        }

        private async Task Init()
        {
            _config.Validate();

            var consumerProperties = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(_config.Reference))
            {
                consumerProperties["name"] = _config.Reference;
            }

            if (!string.IsNullOrEmpty(_config.Identifier))
            {
                consumerProperties["identifier"] = _config.Identifier;
            }

            if (_config.IsFiltering)
            {
                var i = 0;
                foreach (var filterValue in _config.ConsumerFilter.Values)
                {
                    var k = Consts.SubscriptionPropertyFilterPrefix + i++;
                    consumerProperties[k] = filterValue;
                }

                consumerProperties[Consts.SubscriptionPropertyMatchUnfiltered] =
                    _config.ConsumerFilter.MatchUnfiltered.ToString().ToLower();
            }

            if (_config.IsSingleActiveConsumer)
            {
                consumerProperties["single-active-consumer"] = "true";
                if (!string.IsNullOrEmpty(_config.SuperStream))
                {
                    consumerProperties["super-stream"] = _config.SuperStream;
                }
            }

            var chunkConsumed = 0;
            // this the default value for the consumer.
            _config.StoredOffsetSpec = _config.OffsetSpec;
            _status = EntityStatus.Initializing;
            (EntityId, var response) = await _client.Subscribe(
                _config,
                _initialCredits,
                consumerProperties,
                async deliver =>
                {
                    try
                    {
                        chunkConsumed++;
                        // Send the chunk to the _chunksBuffer
                        // in this way the chunks are processed in a separate thread
                        // this wont' block the socket thread
                        // introduced https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/250
                        if (Token.IsCancellationRequested)
                        {
                            // the consumer is closing from the user but some chunks are still in the buffer
                            // simply skip the chunk
                            Logger?.LogTrace(
                                "CancellationToken requested. The {EntityInfo} " +
                                "The chunk won't be processed",
                                DumpEntityConfiguration());
                            return;
                        }

                        var skipChunk = ChunkAction.TryToProcess;

                        if (_config.Crc32 is not null)
                        {
                            var crcCalculated = BitConverter.ToUInt32(
                                _config.Crc32.Hash(deliver.Chunk.Data.ToArray())
                            );
                            if (crcCalculated != deliver.Chunk.Crc)
                            {
                                Logger?.LogError(
                                    "CRC32 does not match, server crc: {Crc}, local crc: {CrcCalculated}, {EntityInfo}, " +
                                    "Chunk Consumed {ChunkConsumed}", deliver.Chunk.Crc, crcCalculated,
                                    DumpEntityConfiguration(),
                                    chunkConsumed);

                                if (_config.Crc32.FailAction != null)
                                {
                                    // if the user has set the FailAction, we call it
                                    // to allow the user to handle the chunk action
                                    skipChunk = _config.Crc32.FailAction(this);
                                }
                            }
                        }

                        await _chunksBuffer.Writer.WriteAsync((deliver.Chunk, skipChunk), Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // The consumer is closing from the user but some chunks are still in the buffer
                        // simply skip the chunk since the Token.IsCancellationRequested is true
                        // the catch is needed to avoid to propagate the exception to the socket thread.
                        Logger?.LogWarning(
                            "OperationCanceledException. {EntityInfo} has been closed while consuming messages. " +
                            "Token.IsCancellationRequested: {IsCancellationRequested}",
                            DumpEntityConfiguration(), Token.IsCancellationRequested);
                    }
                }, async promotedAsActive =>
                {
                    if (_config.ConsumerUpdateListener != null)
                    {
                        // in this case the StoredOffsetSpec is overridden by the ConsumerUpdateListener
                        // since the user decided to override the default behavior
                        await MaybeLockDispatch().ConfigureAwait(false);
                        try
                        {
                            _config.StoredOffsetSpec = await _config.ConsumerUpdateListener(
                                _config.Reference,
                                _config.Stream,
                                promotedAsActive).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Logger?.LogError(e,
                                "Error while calling the ConsumerUpdateListener. OffsetTypeNext will be used. {EntityInfo}",
                                DumpEntityConfiguration());
                            // in this case the default behavior is to use the OffsetTypeNext
                            _config.StoredOffsetSpec = new OffsetTypeNext();
                        }
                        finally
                        {
                            MaybeReleaseLock();
                        }
                    }

                    // Here we set the promotion status
                    // important for the dispatcher messages 
                    IsPromotedAsActive = promotedAsActive;
                    Logger?.LogDebug(
                        "The consumer active status is: {IsActive} for {ConsumeInfo}",
                        IsPromotedAsActive,
                        DumpEntityConfiguration());
                    return _config.StoredOffsetSpec;
                }
            ).ConfigureAwait(false);

            if (response.ResponseCode == ResponseCode.Ok)
            {
                _client.ConnectionClosed += OnConnectionClosed();
                _client.Parameters.OnMetadataUpdate += OnMetadataUpdate();

                _status = EntityStatus.Open;
                // the subscription is completed so the parsechunk can start to process the chunks
                _completeSubscription.SetResult();
                return;
            }

            throw new CreateConsumerException($"consumer could not be created code: {response.ResponseCode}",
                response.ResponseCode);
        }

        private ClientParameters.MetadataUpdateHandler OnMetadataUpdate() =>
            async metaDataUpdate =>
            {
                // the connection can handle different streams
                // we need to check if the metadata update is for the stream
                // where the consumer is consuming else can ignore the update
                if (metaDataUpdate.Stream != _config.Stream)
                    return;
                // remove the event since the consumer is closed
                // only if the stream is the valid

                _client.ConnectionClosed -= OnConnectionClosed();
                _client.Parameters.OnMetadataUpdate -= OnMetadataUpdate();

                // at this point the server has removed the consumer from the list 
                // and the unsubscribe is not needed anymore (ignoreIfClosed = true)
                // we call the Close to re-enter to the standard behavior
                // ignoreIfClosed is an optimization to avoid to send the unsubscribe
                _config.Pool.RemoveConsumerEntityFromStream(_client.ClientId, EntityId, _config.Stream);
                await Shutdown(_config, true).ConfigureAwait(false);
                _config.MetadataHandler?.Invoke(metaDataUpdate);
            };

        private Client.ConnectionCloseHandler OnConnectionClosed() =>
            async reason =>
            {
                _client.ConnectionClosed -= OnConnectionClosed();
                _client.Parameters.OnMetadataUpdate -= OnMetadataUpdate();

                // remove the event since the connection is closed
                _config.Pool.Remove(_client.ClientId);
                UpdateStatusToClosed();
                if (_config.ConnectionClosedHandler != null)
                {
                    await _config.ConnectionClosedHandler(reason).ConfigureAwait(false);
                }
            };

        protected override async Task<ResponseCode> DeleteEntityFromTheServer(bool ignoreIfAlreadyDeleted = false)
        {
            try
            {
                var unsubscribeResponse =
                    await _client.Unsubscribe(EntityId, ignoreIfAlreadyDeleted).ConfigureAwait(false);
                return unsubscribeResponse.ResponseCode;
            }

            catch (TimeoutException)
            {
                Logger.LogError(
                    "Timeout removing the consumer id: {SubscriberId}, {EntityInfo} from the server. " +
                    "The consumer will be closed anyway",
                    EntityId, DumpEntityConfiguration());
            }

            catch (Exception e)
            {
                Logger.LogError(e,
                    "Error removing {EntityInfo} from the server",
                    DumpEntityConfiguration());
            }

            return ResponseCode.Ok;
        }

        public override async Task<ResponseCode> Close()
        {
            // when the consumer is closed we must be sure that the 
            // the subscription is completed to avoid problems with the connection
            // It could happen when the closing is called just after the creation
            _completeSubscription.Task.Wait();
            return await Shutdown(_config).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _completeSubscription.Task.Wait();
            try
            {
                Dispose(true);
            }
            finally
            {
                GC.SuppressFinalize(this);
            }
        }

        public ConsumerInfo Info { get; }
    }
}
