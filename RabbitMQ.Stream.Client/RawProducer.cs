// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

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

        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };

        public RawProducerConfig(string stream)
        {
            if (string.IsNullOrWhiteSpace(stream))
            {
                throw new ArgumentException("Stream cannot be null or whitespace.", nameof(stream));
            }

            Stream = stream;
        }
    }

    public class RawProducer : AbstractEntity, IProducer, IDisposable
    {
        private bool _disposed;
        private byte _publisherId;
        private readonly RawProducerConfig _config;
        private readonly Channel<OutgoingMsg> _messageBuffer;
        private readonly SemaphoreSlim _semaphore;
        private readonly ILogger _logger;

        public int PendingCount => _config.MaxInFlight - _semaphore.CurrentCount;

        public static async Task<IProducer> Create(
            ClientParameters clientParameters,
            RawProducerConfig config,
            StreamInfo metaStreamInfo,
            ILogger logger = null
        )
        {
            var client = await RoutingHelper<Routing>.LookupLeaderConnection(clientParameters, metaStreamInfo, logger).ConfigureAwait(false);

            var producer = new RawProducer((Client)client, config, logger);
            await producer.Init().ConfigureAwait(false);
            return producer;
        }

        private RawProducer(Client client, RawProducerConfig config, ILogger logger = null)
        {
            _client = client;
            _config = config;
            _messageBuffer = Channel.CreateBounded<OutgoingMsg>(new BoundedChannelOptions(10000)
            {
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
            _logger = logger ?? NullLogger.Instance;
            Task.Run(ProcessBuffer);
            _semaphore = new SemaphoreSlim(config.MaxInFlight, config.MaxInFlight);
        }

        public int MessagesSent => _client.MessagesSent;
        public int ConfirmFrames => _client.ConfirmFrames;
        public int IncomingFrames => _client.IncomingFrames;
        public int PublishCommandsSent => _client.PublishCommandsSent;

        private async Task Init()
        {
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

            var (pubId, response) = await _client.DeclarePublisher(
                _config.Reference,
                _config.Stream,
                publishingIds =>
                {
                    foreach (var id in publishingIds.Span)
                    {
                        _config.ConfirmHandler(new Confirmation
                        {
                            PublishingId = id,
                            Code = ResponseCode.Ok,
                            Stream = _config.Stream
                        });
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
                });

            if (response.ResponseCode == ResponseCode.Ok)
            {
                _publisherId = pubId;
                return;
            }

            throw new CreateProducerException($"producer could not be created code: {response.ResponseCode}");
        }

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
            if (subEntryMessages.Count != 0)
            {
                await SemaphoreAwaitAsync();
                var publishTask =
                    _client.Publish(new SubEntryPublish(_publisherId, publishingId,
                        CompressionHelper.Compress(subEntryMessages, compressionType)));
                await publishTask;
            }
        }

        private async Task SemaphoreAwaitAsync()
        {
            await _semaphore.WaitAsync(Token);
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
            PreValidateBatch(messages);
            await InternalBatchSend(messages);
        }

        internal async Task InternalBatchSend(List<(ulong, Message)> messages)
        {
            for (var i = 0; i < messages.Count; i++)
            {
                await SemaphoreAwaitAsync();
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
            await _client.Publish(new Publish(_publisherId, messages));
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
            var response = await _client.QueryPublisherSequence(_config.Reference, _config.Stream);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"GetLastPublishingId stream: {_config.Stream}, reference: {_config.Reference}");
            return response.Sequence;
        }

        public bool IsOpen()
        {
            return !_disposed && !_client.IsClosed;
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
            if (message.Size > _client.MaxFrameSize)
            {
                throw new InvalidOperationException($"Message size is to big. " +
                                                    $"Max allowed is {_client.MaxFrameSize}");
            }

            await SemaphoreAwaitAsync();
            var msg = new OutgoingMsg(_publisherId, publishingId, message);

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
                _logger.LogError(e, "error while Process Buffer");
            }
        }

        public async Task<ResponseCode> Close()
        {
            // This unlocks the semaphore so that the background task can exit
            // see SemaphoreAwaitAsync method and processBuffer method
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
                // the producer could be removed because of stream deleted 
                // so it is not necessary to wait.
                var closeResponse = await _client.DeletePublisher(_publisherId).WaitAsync(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
                result = closeResponse.ResponseCode;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error removing the producer id: {PublisherId} from the server", _publisherId);
            }

            var closed = await _client.MaybeClose($"client-close-publisher: {_publisherId}").ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"client-close-publisher: {_publisherId}");
            _logger?.LogDebug("Publisher {PublisherId} closed", _publisherId);
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

            var closeProducer = Close();
            closeProducer.Wait(TimeSpan.FromSeconds(1));
            ClientExceptions.MaybeThrowException(closeProducer.Result,
                $"Error during remove producer. Producer: {_publisherId}");
            _disposed = true;
        }

        public void Dispose()
        {
            try
            {
                Dispose(true);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error during disposing Consumer: {PublisherId}.", _publisherId);
            }

            GC.SuppressFinalize(this);
        }
    }
}
