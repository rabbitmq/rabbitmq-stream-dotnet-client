// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client;

public class RawSuperStreamConsumer : IConsumer, IDisposable
{
    // ConcurrentDictionary because the consumer can be closed from another thread
    // The send operations will check if the producer exists and if not it will be created
    private readonly ConcurrentDictionary<string, IConsumer> _consumers = new();
    private bool _disposed;

    private readonly RawSuperStreamConsumerConfig _config;

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
    public static IConsumer Create(
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
            ConnectionClosedHandler = async (string s) =>
            {
                // if the stream is still in the consumer list
                // means that the consumer was not closed voluntarily
                // and it is needed to recreate it.
                // The stream will be removed from the list when the consumer is closed
                if (_consumers.ContainsKey(stream))
                {
                    _logger.LogInformation(
                        "Consumer {ConsumerReference} is disconnected from {StreamIdentifier}. Client will try reconnect",
                        _config.Reference,
                        stream
                    );
                    _consumers.TryRemove(stream, out _);
                    await GetConsumer(stream);
                }
            },
            MessageHandler = async (consumer, context, message) =>
            {
                // in the message handler we need to add also the source stream
                // since there could be multiple streams (one per partition)
                // it is useful client side to know from which stream the message is coming from
                if (_config.MessageHandler != null)
                {
                    await _config.MessageHandler(stream, consumer, context, message);
                }
            },
            MetadataHandler = async update =>
            {
                // In case of stream update we remove the producer from the list
                // We hide the behavior of the producer to the user
                // if needed the connection will be created again
                // we "should" always have the stream

                // but we have to handle the case¬
                // We need to wait a bit it can take some time to update the configuration
                Thread.Sleep(500);

                _streamInfos.Remove(update.Stream);
                _consumers.TryRemove(update.Stream, out var consumerMetadata);
                consumerMetadata?.Close();

                // this check is needed only for an edge case 
                // when the system is closed and the connections for the steam are still open for
                // some reason. So if the Client IsClosed we can't operate on it
                if (_config.Client.IsClosed || _disposed)
                {
                    return;
                }

                var exists = _config.Client.StreamExists(update.Stream);
                if (!exists.Result)
                {
                    // The stream doesn't exist anymore
                    // but this condition should be avoided since the hash routing 
                    // can be compromised
                    _logger.LogWarning("SuperStream Consumer. Stream {StreamIdentifier} is not available anymore",
                        update.Stream);
                }
                else
                {
                    await Task.Run(async () =>
                    {
                        // this is an edge case when the user remove a replica for the stream
                        // s0 the topology is changed and the consumer is disconnected
                        // this is why in this case we need to query the QueryMetadata again
                        // most of the time this code is not executed
                        _logger.LogInformation(
                            "Consumer: {ConsumerReference}. Metadata update for stream {StreamIdentifier}. Client will try reconnect",
                            _config.Reference,
                            update.Stream
                        );
                        var x = await _config.Client.QueryMetadata(new[] { update.Stream });
                        x.StreamInfos.TryGetValue(update.Stream, out var streamInfo);
                        _streamInfos.Add(update.Stream, streamInfo);
                        await GetConsumer(update.Stream);
                    });
                }
            },
            OffsetSpec = _config.OffsetSpec.ContainsKey(stream) ? _config.OffsetSpec[stream] : new OffsetTypeNext(),
        };
    }

    private async Task<IConsumer> InitConsumer(string stream)
    {
        var c = await RawConsumer.Create(
            _clientParameters with { ClientProvidedName = _clientParameters.ClientProvidedName },
            FromStreamConfig(stream), _streamInfos[stream], _logger);
        _logger?.LogDebug("Consumer {ConsumerReference} created for Stream {StreamIdentifier}", _config.Reference,
            stream);
        return c;
    }

    private async Task<IConsumer> GetConsumer(string stream)
    {
        if (!_consumers.ContainsKey(stream))
        {
            var p = await InitConsumer(stream);
            _consumers.TryAdd(stream, p);
        }

        return _consumers[stream];
    }

    private async Task StartConsumers()
    {
        foreach (var stream in _streamInfos.Keys)
        {
            await GetConsumer(stream);
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
            _consumers.TryRemove(stream, out var consumer);
            consumer?.Close();
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }
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

    public string SuperStream { get; }

    internal Client Client { get; set; }
}
