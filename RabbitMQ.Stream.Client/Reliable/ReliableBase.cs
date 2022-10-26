// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableConfig
{
    public IReconnectStrategy ReconnectStrategy { get; set; } = new BackOffReconnectStrategy();
    public StreamSystem StreamSystem { get; }
    public string Stream { get; }

    protected ReliableConfig(StreamSystem streamSystem, string stream)
    {
        if (string.IsNullOrWhiteSpace(stream))
        {
            throw new ArgumentException("Stream cannot be null or whitespace.", nameof(stream));
        }

        Stream = stream;
        StreamSystem = streamSystem ?? throw new ArgumentNullException(nameof(streamSystem));
    }
}

/// <summary>
/// Base class for Reliable producer/ consumer
/// With the term Entity we mean a Producer or a Consumer
/// </summary>
public abstract class ReliableBase
{
    protected readonly SemaphoreSlim SemaphoreSlim = new(1);

    protected bool _isOpen;
    protected bool _inReconnection;

    internal async Task Init(IReconnectStrategy reconnectStrategy)
    {
        await Init(true, reconnectStrategy);
    }

    // <summary>
    /// Init the reliable client
    /// <param name="boot"> If it is the First boot for the reliable P/C </param>
    /// <param name="reconnectStrategy">IReconnectStrategy</param>
    /// Try to Init the Entity, if it fails, it will try to reconnect
    /// only if the exception is a known exception
    // </summary>
    private async Task Init(bool boot, IReconnectStrategy reconnectStrategy)
    {
        var reconnect = false;
        await SemaphoreSlim.WaitAsync();
        try
        {
            _isOpen = true;
            await CreateNewEntity(boot);
        }

        catch (Exception e)
        {
            reconnect = IsAKnownException(e);
            LogException(e);
            if (!reconnect)
            {
                // We consider the client as closed
                // since the exception is raised to the caller
                _isOpen = false;
                throw;
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }

        if (reconnect)
        {
            await TryToReconnect(reconnectStrategy);
        }
    }

    // <summary>
    /// Init the a new Entity (Producer/Consumer)
    /// <param name="boot"> If it is the First boot for the reliable P/C </param>
    /// Called by Init method
    // </summary>
    internal abstract Task CreateNewEntity(bool boot);

    // <summary>
    /// Try to reconnect to the broker
    /// Based on the retry strategy
    /// <param name="reconnectStrategy"> The Strategy for the reconnection
    /// by default it is exponential backoff.
    /// It it possible to change implementing the IReconnectStrategy interface </param>
    // </summary>
    protected async Task TryToReconnect(IReconnectStrategy reconnectStrategy)
    {
        _inReconnection = true;
        try
        {
            switch (await reconnectStrategy.WhenDisconnected(ToString()) && _isOpen)
            {
                case true:
                    LogEventSource.Log.LogInformation(
                        $"{ToString()} is disconnected. Client will try reconnect");
                    await Init(false, reconnectStrategy);
                    break;
                case false:
                    LogEventSource.Log.LogInformation(
                        $"{ToString()} is asked to be closed");
                    await Close();
                    break;
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

            await CloseEntity();
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

    // <summary>
    /// IsAKnownException returns true if the exception is a known exception
    /// We need it to reconnect when the producer/consumer.
    /// - LeaderNotFoundException is a temporary exception
    ///   It means that the leader is not available and the client can't reconnect.
    ///   Especially the Producer that needs to know the leader.
    /// - SocketException
    ///   Client is trying to connect in a not ready endpoint.
    ///   It is usually a temporary situation.
    /// -  TimeoutException
    ///    Some call went in timeout. Maybe a temporary DNS problem.
    ///    In this case we can try to reconnect.
    ///
    ///  For the other kind of exception, we just throw back the exception.
    //</summary>
    internal static bool IsAKnownException(Exception exception)
    {
        return exception is (SocketException or TimeoutException or LeaderNotFoundException);
    }

    private void LogException(Exception exception)
    {
        LogEventSource.Log.LogError(IsAKnownException(exception)
            ? $"Trying to reconnect {ToString()} due of: {exception.Message}"
            : $"Error during initialization {ToString()} due of: {exception.Message}");
    }

    // <summary>
    /// ONLY close the current Entity (Producer/Consumer)
    /// without closing the Reliable(Producer/Consumer) instance.
    /// It happens when the stream change topology, and the entity 
    /// must be recreated. In the producer case for example when the
    /// leader changes.
    // </summary>
    protected abstract Task CloseEntity();

    // <summary>
    /// Close the Reliable(Producer/Consumer) instance.
    // </summary>
    public abstract Task Close();

    public bool IsOpen()
    {
        return _isOpen;
    }
}
