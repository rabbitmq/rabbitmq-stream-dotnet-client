// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client;

public class RawSuperStreamConsumer : ISuperStreamConsumer, IDisposable
{
    // ConcurrentDictionary because the consumer can be closed from another thread
    // The send operations will check if the producer exists and if not it will be created
    private readonly ConcurrentDictionary<string, IConsumer> _consumers = new();
    private bool _disposed;

    private readonly RawSuperStreamConsumerConfig _config;
    private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

    //  Contains the info about the streams (one per partition)
    private readonly IDictionary<string, StreamInfo> _streamInfos;
    private readonly ClientParameters _clientParameters;
    private readonly ILogger _logger;

    /// <summary>
    /// Create a new super stream consumer
    /// </summary>
    /// <param name="rawSuperStreamConsumerConfig"></param>
    /// <param name="streamInfos"></param>
    /// <param name="clientParameters"></param>
    /// <param name="logger"></param>
    /// <returns></returns>
    public static ISuperStreamConsumer Create(
        RawSuperStreamConsumerConfig rawSuperStreamConsumerConfig,
        IDictionary<string, StreamInfo> streamInfos,
        ClientParameters clientParameters,
        ILogger logger = null
    )
    {
        return new RawSuperStreamConsumer(rawSuperStreamConsumerConfig, streamInfos, clientParameters, logger);
    }

    private RawSuperStreamConsumer(
        RawSuperStreamConsumerConfig config,
        IDictionary<string, StreamInfo> streamInfos,
        ClientParameters clientParameters,
        ILogger logger = null
    )
    {
        _config = config;
        _streamInfos = streamInfos;
        _clientParameters = clientParameters;
        _logger = logger ?? NullLogger.Instance;
        Info = new ConsumerInfo(_config.SuperStream, _config.Reference, config.Identifier, _streamInfos.Keys.ToList());

        StartConsumers().Wait(CancellationToken.None);
    }

    // We need to copy the config from the super consumer to the standard consumer
    private RawConsumerConfig FromStreamConfig(string stream)
    {
        return new RawConsumerConfig(stream)
        {
            Reference = _config.Reference,
            SuperStream = _config.SuperStream,
            IsSingleActiveConsumer = _config.IsSingleActiveConsumer,
            ConsumerUpdateListener = _config.ConsumerUpdateListener,
            ConsumerFilter = _config.ConsumerFilter,
            Pool = _config.Pool,
            Crc32 = _config.Crc32,
            Identifier = _config.Identifier,
            ConnectionClosedHandler = async (reason) =>
            {
                _consumers.TryRemove(stream, out var consumer);
                if (reason == ConnectionClosedReason.Normal)
                {
                    _logger.LogDebug(
                        "Super Stream consumer {@ConsumerInfo} is closed normally from {StreamIdentifier}",
                        consumer?.Info,
                        stream
                    );
                }
                else
                {
                    _logger.LogWarning(
                        "Super Stream consumer {@ConsumerInfo} is disconnected from {StreamIdentifier} reason: {Reason}",
                        consumer?.Info,
                        stream, reason
                    );
                }

                consumer?.Dispose();

                if (_config.ConnectionClosedHandler != null)
                {
                    await _config.ConnectionClosedHandler(reason, stream).ConfigureAwait(false);
                }
            },
            MessageHandler = async (consumer, context, message) =>
            {
                // in the message handler we need to add also the source stream
                // since there could be multiple streams (one per partition)
                // it is useful client side to know from which stream the message is coming from
                _config.OffsetSpec[stream] = new OffsetTypeOffset(context.Offset);
                if (_config.MessageHandler != null)
                {
                    await _config.MessageHandler(stream, consumer, context, message).ConfigureAwait(false);
                }
            },
            MetadataHandler = async update =>
            {
                _consumers.TryRemove(update.Stream, out var consumer);
                consumer?.Close();
                if (_config.MetadataHandler != null)
                {
                    await _config.MetadataHandler(update).ConfigureAwait(false);
                }
            },
            OffsetSpec = _config.OffsetSpec.TryGetValue(stream, out var value) ? value : new OffsetTypeNext(),
        };
    }

    private async Task<IConsumer> InitConsumer(string stream)
    {
        var index = _streamInfos.Keys.Select((item, index) => new { Item = item, Index = index })
            .First(i => i.Item == stream).Index;

        var c = await RawConsumer.Create(
            _clientParameters with { ClientProvidedName = $"{_clientParameters.ClientProvidedName}_{index}" },
            FromStreamConfig(stream), _streamInfos[stream], _logger).ConfigureAwait(false);
        _logger?.LogDebug("Super stream consumer {ConsumerReference} created for Stream {StreamIdentifier}", c.Info,
            stream);
        return c;
    }

    private async Task MaybeAddConsumer(string stream)
    {
        if (!_consumers.ContainsKey(stream))
        {
            var p = await InitConsumer(stream).ConfigureAwait(false);
            _consumers.TryAdd(stream, p);
        }
    }

    private async Task StartConsumers()
    {
        foreach (var stream in _streamInfos.Keys)
        {
            await MaybeAddConsumer(stream).ConfigureAwait(false);
        }
    }

    public async Task ReconnectPartition(StreamInfo streamInfo)
    {
        ClientExceptions.CheckLeader(streamInfo);
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            _consumers.TryRemove(streamInfo.Stream, out var consumer);
            consumer?.Dispose();
            _streamInfos[streamInfo.Stream] = streamInfo;
            await MaybeAddConsumer(streamInfo.Stream).ConfigureAwait(false);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    /// <summary>
    /// It is not possible to close store the offset here since the consumer is not aware of the stream
    /// you need to use the consumer inside the MessageHandler to store the offset
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Task StoreOffset(ulong offset)
    {
        throw new NotImplementedException("use the store offset on the stream consumer, instead");
    }

    public Task<ResponseCode> Close()
    {
        if (_disposed)
        {
            Task.FromResult(ResponseCode.Ok);
        }

        Dispose();
        return Task.FromResult(ResponseCode.Ok);
    }

    public void Dispose()
    {
        foreach (var stream in _consumers.Keys)
        {
            _consumers.TryGetValue(stream, out var consumer);
            consumer?.Dispose();
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    public ConsumerInfo Info { get; }
}

public record RawSuperStreamConsumerConfig : IConsumerConfig
{
    public RawSuperStreamConsumerConfig(string superStream)
    {
        if (string.IsNullOrWhiteSpace(superStream))
        {
            throw new ArgumentException("SuperStream name cannot be null or empty", nameof(superStream));
        }

        SuperStream = superStream;
    }

    /// <summary>
    /// the offset spec for each stream
    /// the user can specify the offset for each stream
    /// </summary>
    public ConcurrentDictionary<string, IOffsetType> OffsetSpec { get; set; } = new();

    /// <summary>
    /// MessageHandler is called when a message is received
    /// The first parameter is the stream name from which the message is coming from
    /// </summary>
    public Func<string, RawConsumer, MessageContext, Message, Task> MessageHandler { get; set; }

    public Func<string, string, Task> ConnectionClosedHandler { get; set; }
    public string SuperStream { get; }

    internal Client Client { get; set; }
}
