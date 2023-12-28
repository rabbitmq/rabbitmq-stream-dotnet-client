// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableConfig
{
    public IReconnectStrategy ReconnectStrategy { get; set; }
    public StreamSystem StreamSystem { get; }
    public string Stream { get; }

    protected ReliableConfig(StreamSystem streamSystem, string stream)
    {
        if (string.IsNullOrWhiteSpace(stream))
        {
            throw new ArgumentException("Stream cannot be null or whitespace.", nameof(stream));
        }

        ArgumentNullException.ThrowIfNull(streamSystem);

        Stream = stream;
        StreamSystem = streamSystem;
    }
}

/// <summary>
/// Base class for Reliable producer/ consumer
/// With the term Entity we mean a Producer or a Consumer
/// </summary>
public abstract class ReliableBase
{
    protected readonly SemaphoreSlim SemaphoreSlim = new(1);
    private readonly object _lock = new();
    protected bool _isOpen;
    protected bool _inReconnection;

    protected abstract ILogger BaseLogger { get; }

    internal async Task Init(IReconnectStrategy reconnectStrategy)
    {
        await Init(true, reconnectStrategy).ConfigureAwait(false);
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
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            lock (_lock)
            {
                _isOpen = true;
            }

            await CreateNewEntity(boot).ConfigureAwait(false);
        }

        catch (Exception e)
        {
            reconnect = ClientExceptions.IsAKnownException(e);
            LogException(e);
            if (!reconnect)
            {
                // We consider the client as closed
                // since the exception is raised to the caller
                lock (_lock)
                {
                    _isOpen = false;
                }

                throw;
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }

        if (reconnect)
        {
            await TryToReconnect(reconnectStrategy).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Init the a new Entity (Producer/Consumer)
    /// <param name="boot"> If it is the First boot for the reliable P/C </param>
    /// Called by Init method
    /// </summary>
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
            switch (await reconnectStrategy.WhenDisconnected(ToString()).ConfigureAwait(false) && IsOpen())
            {
                case true:
                    BaseLogger.LogInformation("{Identity} is disconnected. Client will try reconnect", ToString());
                    await Init(false, reconnectStrategy).ConfigureAwait(false);
                    break;
                case false:
                    BaseLogger.LogInformation("{Identity} is asked to be closed", ToString());
                    await Close().ConfigureAwait(false);
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
        await Task.Delay(500).ConfigureAwait(false);
        if (await system.StreamExists(stream).ConfigureAwait(false))
        {
            BaseLogger.LogInformation(
                "Meta data update stream: {StreamIdentifier}. The stream still exists. Client: {Identity}",
                stream,
                ToString()
            );
            // Here we just close the producer connection
            // the func TryToReconnect/0 will be called. 

            await CloseEntity().ConfigureAwait(false);
        }
        else
        {
            // In this case the stream doesn't exist anymore
            // the ReliableProducer is just closed.
            BaseLogger.LogInformation("Meta data update stream: {StreamIdentifier}. {Identity} will be closed",
                stream,
                ToString()
            );
            await Close().ConfigureAwait(false);
        }
    }

    private void LogException(Exception exception)
    {
        const string KnownExceptionTemplate = "{Identity} trying to reconnect due to exception";
        const string UnknownExceptionTemplate = "{Identity} received an exception during initialization";
        if (ClientExceptions.IsAKnownException(exception))
        {
            BaseLogger.LogError(exception, KnownExceptionTemplate, ToString());
        }
        else
        {
            BaseLogger.LogError(exception, UnknownExceptionTemplate, ToString());
        }
    }

    /// <summary>
    /// ONLY close the current Entity (Producer/Consumer)
    /// without closing the Reliable(Producer/Consumer) instance.
    /// It happens when the stream change topology, and the entity 
    /// must be recreated. In the producer case for example when the
    /// leader changes.
    /// </summary>
    protected abstract Task CloseEntity();

    // <summary>
    /// Close the Reliable(Producer/Consumer) instance.
    // </summary>
    public abstract Task Close();

    public bool IsOpen()
    {
        lock (_lock)
        {
            return _isOpen;
        }
    }
}
