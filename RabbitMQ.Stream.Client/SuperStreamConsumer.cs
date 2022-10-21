// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public class SuperStreamConsumer : IConsumer, IDisposable
{
    // ConcurrentDictionary because the consumer can be closed from another thread
    // The send operations will check if the producer exists and if not it will be created
    private readonly ConcurrentDictionary<string, IConsumer> _consumers = new();
    private bool _disposed;
    private readonly SuperStreamConsumerConfig _config;
    private readonly IDictionary<string, StreamInfo> _streamInfos;
    private readonly ClientParameters _clientParameters;

    // We need to copy the config from the super consumer to the standard consumer

    private ConsumerConfig FromStreamConfig(string stream)
    {
        return new ConsumerConfig()
        {
            Stream = stream,
            Reference = _config.Reference,
            IsSingleActiveConsumer = _config.IsSingleActiveConsumer,
            ConsumerUpdateListener = _config.ConsumerUpdateListener,
            ConnectionClosedHandler = async (string s) =>
            {
                if (_consumers.ContainsKey(stream))
                {
                    LogEventSource.Log.LogInformation(
                        $"Super Stream Consumer. Consumer {_config.Reference} is disconnected from {stream}. Client will try reconnect");
                    _consumers.TryRemove(stream, out _);
                    await GetConsumer(stream);
                }
            },
            MessageHandler = async (consumer, context, message) =>
            {
                await _config.MessageHandler(stream, consumer, context, message);
            },
            MetadataHandler = update =>
            {
                // In case of stream update we remove the producer from the list
                // We hide the behavior of the producer to the user
                // if needed the connection will be created again
                // we "should" always have the stream

                // but we have to handle the case¬
                // We need to wait a bit it can take some time to update the configuration
                Thread.Sleep(500);

                var exists = _config.Client.StreamExists(update.Stream);
                if (!exists.Result)
                {
                    // The stream doesn't exist anymore
                    // but this condition should be avoided since the hash routing 
                    // can be compromised
                    LogEventSource.Log.LogWarning(
                        $"SuperStream Consumer. Stream {update.Stream} is not available anymore");
                    _streamInfos.Remove(update.Stream);
                    _consumers[update.Stream].Close();
                    _consumers.TryRemove(update.Stream, out _);
                }
                else
                {
                    LogEventSource.Log.LogInformation(
                        $"Super Stream Consumer. {_config.Reference}. Metadata update for stream {update.Stream}. Client will try reconnect");
                    _consumers.TryRemove(update.Stream, out _);
                    GetConsumer(update.Stream).WaitAsync(CancellationToken.None);
                }
            },
            OffsetSpec = _config.OffsetSpec.ContainsKey(stream) ? _config.OffsetSpec[stream] : new OffsetTypeNext(),
        };
    }

    private async Task<IConsumer> InitConsumer(string stream)
    {
        var c = await Consumer.Create(_clientParameters with { ClientProvidedName = _clientParameters.ClientProvidedName },
            FromStreamConfig(stream), _streamInfos[stream]);
        LogEventSource.Log.LogInformation($"SuperStream Consumer. Consumer {_config.Reference} created for Stream {stream}");
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

    private SuperStreamConsumer(SuperStreamConsumerConfig config,
        IDictionary<string, StreamInfo> streamInfos, ClientParameters clientParameters)
    {
        _config = config;
        _streamInfos = streamInfos;
        _clientParameters = clientParameters;

        StartConsumers().Wait(CancellationToken.None);
    }

    private async Task StartConsumers()
    {
        foreach (var stream in _streamInfos.Keys)
        {
            await GetConsumer(stream);
        }
    }

    public static IConsumer Create(SuperStreamConsumerConfig superStreamConsumerConfig,
        IDictionary<string, StreamInfo> streamInfos, ClientParameters clientParameters)
    {
        return new SuperStreamConsumer(superStreamConsumerConfig, streamInfos, clientParameters);
    }

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

public record SuperStreamConsumerConfig : IConsumerConfig
{
    public ConcurrentDictionary<string, IOffsetType> OffsetSpec { get; set; } = new();

    public Func<string, Consumer, MessageContext, Message, Task> MessageHandler { get; set; }

    public string SuperStream { get; set; }

    internal Client Client { get; set; }
}
