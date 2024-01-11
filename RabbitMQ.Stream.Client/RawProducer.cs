// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client
{
    public struct Confirmation
    {
        public ulong PublishingId { get; set; }
        public ResponseCode Code { get; set; }

        public string Stream { get; set; }
    }

    public record RawProducerConfig : IProducerConfig
    {
        public string Stream { get; }
        public Func<string, Task> ConnectionClosedHandler { get; set; }
        public Action<Confirmation> ConfirmHandler { get; set; } = _ => { };

        public RawProducerConfig(string stream)
        {
            if (string.IsNullOrWhiteSpace(stream))
            {
                throw new ArgumentException("Stream cannot be null or whitespace.", nameof(Stream));
            }

            Stream = stream;
        }

        internal void Validate()
        {
            if (Filter is { FilterValue: not null } && !AvailableFeaturesSingleton.Instance.PublishFilter)
            {
                throw new UnsupportedOperationException(Consts.FilterNotSupported);
            }
        }
    }

    public class RawProducer : AbstractEntity, IProducer, IDisposable
    {
        private readonly RawProducerConfig _config;
        private readonly Channel<OutgoingMsg> _messageBuffer;
        private readonly SemaphoreSlim _semaphore;

        public int PendingCount => _config.MaxInFlight - _semaphore.CurrentCount;

        protected override string GetStream() => _config.Stream;

        protected override string DumpEntityConfiguration()
        {
            return
                $"Producer id {EntityId} for stream: {_config.Stream}, reference: {_config.Reference}," +
                $"Client ProvidedName {_config.ClientProvidedName}, " +
                $"Token IsCancellationRequested: {Token.IsCancellationRequested} ";
        }

        public static async Task<IProducer> Create(
            ClientParameters clientParameters,
            RawProducerConfig config,
            StreamInfo metaStreamInfo,
            ILogger logger = null
        )
        {
            var client = await RoutingHelper<Routing>
                .LookupLeaderConnection(
                    clientParameters,
                    metaStreamInfo, config.Pool, logger)
                .ConfigureAwait(false);

            var producer = new RawProducer((Client)client, config, logger);
            await producer.Init().ConfigureAwait(false);
            return producer;
        }

        private RawProducer(Client client, RawProducerConfig config, ILogger logger = null)
        {
            _client = client;
            _config = config;
            Info = new ProducerInfo(_config.Stream, _config.Reference);
            _messageBuffer = Channel.CreateBounded<OutgoingMsg>(new BoundedChannelOptions(10000)
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
            Logger = logger ?? NullLogger.Instance;
            Task.Run(ProcessBuffer);
            _semaphore = new SemaphoreSlim(config.MaxInFlight, config.MaxInFlight);
        }

        public int MessagesSent => _client.MessagesSent;
        public int ConfirmFrames => _client.ConfirmFrames;
        public int IncomingFrames => _client.IncomingFrames;
        public int PublishCommandsSent => _client.PublishCommandsSent;

        private async Task Init()
        {
            _config.Validate();

            (EntityId, var response) = await _client.DeclarePublisher(
                _config.Reference,
                _config.Stream,
                publishingIds =>
                {
                    foreach (var id in publishingIds.Span)
                    {
                        try
                        {
                            _config.ConfirmHandler(new Confirmation
                            {
                                PublishingId = id,
                                Code = ResponseCode.Ok,
                                Stream = _config.Stream
                            });
                        }
                        catch (Exception e)
                        {
                            // The call is exposed to the user so we need to catch any exception
                            // there could be an exception in the user code. 
                            // So here we log the exception and we continue.

                            Logger.LogError(e, "Error during confirm handler, publishing id: {Id}. {ProducerInfo} " +
                                               "Hint: Check the user ConfirmHandler callback", id,
                                DumpEntityConfiguration());
                        }
                    }

                    _semaphore.Release(publishingIds.Length);
                },
                errors =>
                {
                    foreach (var (id, code) in errors)
                    {
                        _config.ConfirmHandler(new Confirmation { PublishingId = id, Code = code, });
                    }

                    _semaphore.Release(errors.Length);
                }, _config.Pool).ConfigureAwait(false);

            if (response.ResponseCode == ResponseCode.Ok)
            {
                _client.ConnectionClosed += OnConnectionClosed();
                _client.Parameters.OnMetadataUpdate += OnMetadataUpdate();
                _status = EntityStatus.Open;
                return;
            }

            throw new CreateProducerException($"producer could not be created code: {response.ResponseCode}",
                response.ResponseCode);
        }

        private Client.ConnectionCloseHandler OnConnectionClosed() =>
            async (reason) =>
            {
                _config.Pool.Remove(_client.ClientId);
                await Shutdown(_config, true).ConfigureAwait(false);
                if (_config.ConnectionClosedHandler != null)
                {
                    await _config.ConnectionClosedHandler(reason).ConfigureAwait(false);
                }

                // remove the event since the connection is closed
                _client.ConnectionClosed -= OnConnectionClosed();
            };

        private ClientParameters.MetadataUpdateHandler OnMetadataUpdate() =>
            async metaDataUpdate =>
            {
                // the connection can handle different streams
                // we need to check if the metadata update is for the stream
                // where the producer is sending the messages else can ignore the update
                if (metaDataUpdate.Stream != _config.Stream)
                    return;

                _client.ConnectionClosed -= OnConnectionClosed();
                _client.Parameters.OnMetadataUpdate -= OnMetadataUpdate();

                _config.Pool.RemoveProducerEntityFromStream(_client.ClientId, EntityId, _config.Stream);

                // at this point the server has removed the producer from the list 
                // and the DeletePublisher producer is not needed anymore (ignoreIfClosed = true)
                // we call the Close to re-enter to the standard behavior
                // ignoreIfClosed is an optimization to avoid to send the DeletePublisher
                _config.MetadataHandler?.Invoke(metaDataUpdate);
                await Shutdown(_config, true).ConfigureAwait(false);
            };

        private bool IsFilteringEnabled => _config.Filter is { FilterValue: not null };

        /// <summary>
        /// SubEntry Batch send: Aggregate more messages under the same publishingId.
        /// Relation is publishingId ->[]messages. 
        /// Messages can be compressed using different methods.
        /// Note:
        /// Deduplication is not guaranteed when using sub-entries batching.
        /// </summary>
        /// <param name="publishingId"></param>
        /// <param name="subEntryMessages"> List of messages for sub-entry. Max len allowed is ushort.MaxValue</param>
        /// <param name="compressionType">No Compression, Gzip Compression. Other types are not provided by default</param>
        public async ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
        {
            ThrowIfClosed();
            if (subEntryMessages.Count != 0)
            {
                await SemaphoreAwaitAsync().ConfigureAwait(false);
                var publishTask =
                    _client.Publish(new SubEntryPublish(EntityId, publishingId,
                        CompressionHelper.Compress(subEntryMessages, compressionType)));
                await publishTask.ConfigureAwait(false);
            }
        }

        private async Task SemaphoreAwaitAsync()
        {
            await _semaphore.WaitAsync(Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Send messages in a synchronous way.
        /// This method is needed to be used when you want to send messages in a synchronous way
        /// to control the latency of the messages.
        /// The Send(Message) method is asynchronous the aggregation of messages is done in the background.
        /// </summary>
        /// <param name="messages"></param>
        public async ValueTask Send(List<(ulong, Message)> messages)
        {
            ThrowIfClosed();
            PreValidateBatch(messages);
            await InternalBatchSend(messages).ConfigureAwait(false);
        }

        private async Task InternalBatchSend(List<(ulong, Message)> messages)
        {
            for (var i = 0; i < messages.Count; i++)
            {
                await SemaphoreAwaitAsync().ConfigureAwait(false);
            }

            if (messages.Count != 0 && !_client.IsClosed)
            {
                await SendMessages(messages, false).ConfigureAwait(false);
            }
        }

        internal void PreValidateBatch(List<(ulong, Message)> messages)
        {
            if (messages.Count > _config.MaxInFlight)
            {
                throw new InvalidOperationException($"Too many messages in batch. " +
                                                    $"Max allowed is {_config.MaxInFlight}");
            }

            var totalSize = messages.Sum(message => message.Item2.Size);

            if (totalSize > _client.MaxFrameSize)
            {
                throw new InvalidOperationException($"Total size of messages in batch is too big. " +
                                                    $"Max allowed is {_client.MaxFrameSize}");
            }
        }

        private async Task SendMessages(List<(ulong, Message)> messages, bool clearMessagesList = true)
        {
            ThrowIfClosed();
            if (IsFilteringEnabled)
            {
                await _client.Publish(new PublishFilter(EntityId, messages, _config.Filter.FilterValue,
                        Logger))
                    .ConfigureAwait(false);
            }
            else
            {
                await _client.Publish(new Publish(EntityId, messages)).ConfigureAwait(false);
            }

            if (clearMessagesList)
            {
                messages.Clear();
            }
        }

        /// <summary>
        /// GetLastPublishingId 
        /// </summary>
        /// <returns>The last sequence id stored by the producer.</returns>
        public async Task<ulong> GetLastPublishingId()
        {
            if (string.IsNullOrWhiteSpace(_config.Reference))
            {
                return 0;
            }

            var response = await _client.QueryPublisherSequence(_config.Reference, _config.Stream)
                .ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"GetLastPublishingId stream: {_config.Stream}, reference: {_config.Reference}");
            return response.Sequence;
        }

        /// <summary>
        /// This is the standard way to send messages.
        /// The send is asynchronous and the aggregation of messages is done in the background.
        /// This method can be used for the messages deduplication, if the publishingId is the same the broker will deduplicate the messages.
        /// so only the fist message will be stored the second one will be discarded.
        /// Read the documentation for more details.
        /// <param name="publishingId">The Id for the message and has to be an incremental value.</param>>
        /// <param name="message">Message to store</param>>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask Send(ulong publishingId, Message message)
        {
            ThrowIfClosed();
            if (message.Size > _client.MaxFrameSize)
            {
                throw new InvalidOperationException($"Message size is to big. " +
                                                    $"Max allowed is {_client.MaxFrameSize}");
            }

            await SemaphoreAwaitAsync().ConfigureAwait(false);
            var msg = new OutgoingMsg(EntityId, publishingId, message);

            // Let's see if we can write a message to the channel without having to wait
            if (!_messageBuffer.Writer.TryWrite(msg))
            {
                // Nope, channel is full and being processed, let's asynchronously wait until we can buffer the message
                await _messageBuffer.Writer.WriteAsync(msg, Token).ConfigureAwait(false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task ProcessBuffer()
        {
            try
            {
                var messages = new List<(ulong, Message)>(_config.MessagesBufferSize);
                while (await _messageBuffer.Reader.WaitToReadAsync().ConfigureAwait(false))
                {
                    while (_messageBuffer.Reader.TryRead(out var msg))
                    {
                        messages.Add((msg.PublishingId, msg.Data));
                        if (messages.Count == _config.MessagesBufferSize)
                        {
                            await SendMessages(messages).ConfigureAwait(false);
                        }
                    }

                    if (messages.Count > 0)
                    {
                        await SendMessages(messages).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, "error while Process Buffer");
            }
        }

        public override async Task<ResponseCode> Close()
        {
            return await Shutdown(_config).ConfigureAwait(false);
        }

        protected override async Task<ResponseCode> DeleteEntityFromTheServer(bool ignoreIfAlreadyDeleted = false)
        {
            try
            {
                // The  default timeout is usually 10 seconds 
                // in this case we reduce the waiting time
                // the producer could be removed because of stream deleted 
                // so it is not necessary to wait.
                var closeResponse =
                    await _client.DeletePublisher(EntityId, ignoreIfAlreadyDeleted).ConfigureAwait(false);
                return closeResponse.ResponseCode;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error removing {ProducerInfo} from the server", DumpEntityConfiguration());
            }

            return ResponseCode.Ok;
        }

        public void Dispose()
        {
            try
            {
                Dispose(true);
            }
            finally
            {
                GC.SuppressFinalize(this);
            }
        }

        public ProducerInfo Info { get; }
    }
}
