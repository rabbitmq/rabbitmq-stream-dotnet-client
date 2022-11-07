// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

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

        public int PendingCount => _config.MaxInFlight - _semaphore.CurrentCount;

        private RawProducer(Client client, RawProducerConfig config)
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
            Task.Run(ProcessBuffer);
            _semaphore = new(config.MaxInFlight, config.MaxInFlight);
        }

        public int MessagesSent => _client.MessagesSent;
        public int ConfirmFrames => _client.ConfirmFrames;
        public int IncomingFrames => _client.IncomingFrames;
        public int PublishCommandsSent => _client.PublishCommandsSent;

        private async Task Init()
        {
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
        /// </summary>
        /// <param name="publishingId"></param>
        /// <param name="subEntryMessages"> List of messages for sub-entry. Max len allowed is ushort.MaxValue</param>
        /// <param name="compressionType">No Compression, Gzip Compression. Other types are not provided by default</param>
        public async ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
        {
            if (subEntryMessages.Count != 0)
            {
                await SemaphoreWait();
                var publishTask =
                    _client.Publish(new SubEntryPublish(_publisherId, publishingId,
                        CompressionHelper.Compress(subEntryMessages, compressionType)));
                if (!publishTask.IsCompletedSuccessfully)
                {
                    await publishTask.ConfigureAwait(false);
                }
            }
        }

        public async ValueTask Send(List<(ulong, Message)> messages)
        {
            PreValidateBatch(messages);
            await InternalBatchSend(messages);
        }

        internal async Task InternalBatchSend(List<(ulong, Message)> messages)
        {
            for (var i = 0; i < messages.Count; i++)
            {
                await SemaphoreWait();
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

        private async Task SemaphoreWait()
        {
            if (!await _semaphore.WaitAsync(0) && !_client.IsClosed)
            {
                // Nope, we have maxed our In-Flight messages, let's asynchronously wait for confirms
                if (!await _semaphore.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false))
                {
                    LogEventSource.Log.LogWarning("Semaphore Wait timeout during publishing.");
                }
            }
        }

        private async Task SendMessages(List<(ulong, Message)> messages, bool clearMessagesList = true)
        {
            var publishTask = _client.Publish(new Publish(_publisherId, messages));
            if (!publishTask.IsCompletedSuccessfully)
            {
                await publishTask.ConfigureAwait(false);
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
            var response = await _client.QueryPublisherSequence(_config.Reference, _config.Stream);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"GetLastPublishingId stream: {_config.Stream}, reference: {_config.Reference}");
            return response.Sequence;
        }

        public bool IsOpen()
        {
            return !_disposed && !_client.IsClosed;
        }

        public async ValueTask Send(ulong publishingId, Message message)
        {
            await SemaphoreWait();

            var msg = new OutgoingMsg(_publisherId, publishingId, message);

            // Let's see if we can write a message to the channel without having to wait
            if (!_messageBuffer.Writer.TryWrite(msg))
            {
                // Nope, channel is full and being processed, let's asynchronously wait until we can buffer the message
                await _messageBuffer.Writer.WriteAsync(msg).ConfigureAwait(false);
            }
        }

        private async Task ProcessBuffer()
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

        public async Task<ResponseCode> Close()
        {
            if (_client.IsClosed)
            {
                return ResponseCode.Ok;
            }

            var result = ResponseCode.Ok;
            try
            {
                var deletePublisherResponseTask = _client.DeletePublisher(_publisherId);
                // The  default timeout is usually 10 seconds 
                // in this case we reduce the waiting time
                // the producer could be removed because of stream deleted 
                // so it is not necessary to wait.
                await deletePublisherResponseTask.WaitAsync(TimeSpan.FromSeconds(3));
                if (deletePublisherResponseTask.IsCompletedSuccessfully)
                {
                    result = deletePublisherResponseTask.Result.ResponseCode;
                }
            }
            catch (Exception e)
            {
                LogEventSource.Log.LogError($"Error removing the producer id: {_publisherId} from the server. {e}");
            }

            var closed = _client.MaybeClose($"client-close-publisher: {_publisherId}");
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"client-close-publisher: {_publisherId}");
            return result;
        }

        public static async Task<IProducer> Create(ClientParameters clientParameters,
            RawProducerConfig config,
            StreamInfo metaStreamInfo)
        {
            var client = await RoutingHelper<Routing>.LookupLeaderConnection(clientParameters, metaStreamInfo);
            var producer = new RawProducer((Client)client, config);
            await producer.Init();
            return producer;
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
                LogEventSource.Log.LogError($"Error during disposing Consumer: {_publisherId}.", e);
            }

            GC.SuppressFinalize(this);
        }
    }
}
