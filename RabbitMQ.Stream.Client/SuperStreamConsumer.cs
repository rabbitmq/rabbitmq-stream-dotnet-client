// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    private static ConsumerConfig FromStreamConfig(string stream)
    {
        return new ConsumerConfig() { Stream = stream };
    }

    public Task StoreOffset(ulong offset)
    {
        throw new System.NotImplementedException();
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
        foreach (var (_, iConsumer) in _consumers)
        {
            iConsumer.Close();
        }

        _consumers.Clear();

        _disposed = true;
        GC.SuppressFinalize(this);
    }
}

public record SuperStreamConsumerConfig : IConsumerConfig
{
}
