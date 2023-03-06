// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public class ConsumerIdEventArgs : EventArgs
{
    public ConsumerIdEventArgs(byte consumerId)
    {
        ConsumerId = consumerId;
    }

    public byte ConsumerId { get; }
}

public sealed class RawConsumerAsyncHandler
{
    private readonly BlockingCollection<Chunk> _concurrentQueue = new();

    public RawConsumerAsyncHandler(Action<Chunk> handler, CancellationToken cancellationToken,
        byte consumerId)
    {
        var x = Task.Run(() =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var chunk = _concurrentQueue.Take();
                handler(chunk);
                OnFinished(new ConsumerIdEventArgs(consumerId));
            }
        }, cancellationToken);
    }

    public void AddChunk(Chunk chunk)
    {
        _concurrentQueue.Add(chunk);
    }

    public event EventHandler Finished;

    private void OnFinished(EventArgs consumer)
    {
        Finished?.Invoke(this, consumer);
    }
}
