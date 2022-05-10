// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;
/// <summary>
/// Base class for Reliable for producer/ consumer
/// </summary>
public abstract class ReliableBase
{
    protected readonly SemaphoreSlim SemaphoreSlim = new(1);
    protected bool _needReconnect = true;
    protected bool _inReconnection;

    protected async Task Init()
    {
        await GetNewReliable(true);
    }
    protected abstract Task GetNewReliable(bool boot);

    protected async Task TryToReconnect(IReconnectStrategy reconnectStrategy)
    {
        _inReconnection = true;
        try
        {
            reconnectStrategy.WhenDisconnected(out var reconnect, ToString());
            if (reconnect && _needReconnect)
            {
                LogEventSource.Log.LogInformation(
                    $"{ToString()} is disconnected, try to reconnect");
                await GetNewReliable(false);
            }
        }
        finally
        {
            _inReconnection = false;
        }
    }

    /// <summary>
    /// When the clients receives a meta data update, it doesn't know
    /// the reason.
    /// Metadata update can be raised when:
    /// - stream is deleted
    /// - change the stream topology (ex: add a follower)
    ///
    /// HandleMetaDataMaybeReconnect checks if the stream still exists
    /// and try to reconnect.
    /// (internal because it is needed for tests)
    /// </summary>
    internal async Task HandleMetaDataMaybeReconnect(string stream, StreamSystem system)
    {
        LogEventSource.Log.LogInformation(
            $"Meta data update stream: {stream} " +
            $"{ToString()} closed.");

        // This sleep is needed. When a stream is deleted it takes sometime.
        // The StreamExists/1 could return true even the stream doesn't exist anymore.
        Thread.Sleep(500);
        if (await system.StreamExists(stream))
        {
            LogEventSource.Log.LogInformation(
                $"Meta data update, the stream {stream} still exist. " +
                $"{ToString()} will try to reconnect.");
            // Here we just close the producer connection
            // the func TryToReconnect/0 will be called. 
            await CloseReliable();
        }
        else
        {
            // In this case the stream doesn't exist anymore
            // the ReliableProducer is just closed.
            await Close();
        }
    }

    protected abstract Task CloseReliable();
    public abstract Task Close();

    public bool IsOpen()
    {
        return _needReconnect;
    }
}
