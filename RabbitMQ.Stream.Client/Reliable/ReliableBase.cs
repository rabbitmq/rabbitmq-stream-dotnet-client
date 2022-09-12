// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

/// <summary>
/// Base class for Reliable producer/ consumer
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

    // boot is the first time is called. 
    // used to init the producer/consumer
    protected abstract Task GetNewReliable(bool boot);

    protected async Task TryToReconnect(IReconnectStrategy reconnectStrategy)
    {
        _inReconnection = true;
        try
        {
            var reconnect = await reconnectStrategy.WhenDisconnected(ToString());
            var hasToReconnect = reconnect && _needReconnect;
            var addInfo = "Client won't reconnect";
            if (hasToReconnect)
            {
                addInfo = "Client will try reconnect";
            }

            LogEventSource.Log.LogInformation(
                $"{ToString()} is disconnected. {addInfo}");

            if (hasToReconnect)
            {
                await GetNewReliable(false);
            }
            else
            {
                _needReconnect = false;
                await Close();
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
        // This sleep is needed. When a stream is deleted it takes sometime.
        // The StreamExists/1 could return true even the stream doesn't exist anymore.
        await Task.Delay(500);
        if (await system.StreamExists(stream))
        {
            LogEventSource.Log.LogInformation(
                $"Meta data update stream: {stream}. The stream still exist." +
                $" Client: {ToString()}");
            // Here we just close the producer connection
            // the func TryToReconnect/0 will be called. 
            await CloseReliable();
        }
        else
        {
            // In this case the stream doesn't exist anymore
            // the ReliableProducer is just closed.
            LogEventSource.Log.LogInformation(
                $"Meta data update stream: {stream} " +
                $"{ToString()} will be closed.");
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
