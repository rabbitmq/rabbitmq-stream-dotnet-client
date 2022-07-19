// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

/// <summary>
/// Base class for Reliable producer/ consumer
/// </summary>
public abstract class ReliableBase
{
    protected readonly SemaphoreSlim SemaphoreSlim = new(1);
    public bool IsOpen { get; protected set; } = true;
    protected bool _inReconnection;

    /// <summary>
    /// Try to set a new reliable.
    /// If successful, should set internal variable to a new instance.
    /// Should throw an exception on failure.
    /// </summary>
    /// <param name="boot">Whether this is the first init</param>
    protected abstract Task GetNewReliable(bool boot);

    protected abstract Task CloseReliable();
    public abstract Task Close();

    protected async Task Init()
    {
        await GetNewReliable(true);
    }

    protected async Task TryToReconnect(IReconnectStrategy reconnectStrategy)
    {
        _inReconnection = true;
        try
        {
            var strategySuggestsReconnect = reconnectStrategy.WhenDisconnected(ToString());
            var addInfo = strategySuggestsReconnect ? "Client will try reconnect" : "Client won't reconnect";

            var hasToReconnect = strategySuggestsReconnect && IsOpen;

            LogEventSource.Log.LogInformation($"{ToString()} is disconnected. {addInfo}");


            while (hasToReconnect)
            {
                try
                {
                    await GetNewReliable(false);
                }
                catch (Exception e)
                {
                    LogEventSource.Log.LogError("Got an exception when trying to reconnect", e);
                }

                strategySuggestsReconnect = reconnectStrategy.WhenDisconnected(ToString());
                hasToReconnect = strategySuggestsReconnect && IsOpen;
            }

            if (!strategySuggestsReconnect)
            {
                IsOpen = false;
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
        Thread.Sleep(500);
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
}
