// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client.Reconnect;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableConfig
{
    public IReconnectStrategy ReconnectStrategy { get; set; }
    public IReconnectStrategy ResourceAvailableReconnectStrategy { get; set; }

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

public enum ReliableEntityStatus
{
    Initialization,
    Open,
    Reconnecting,
    Closed,
}

/// <summary>
/// Base class for Reliable producer/ consumer
/// With the term Entity we mean a Producer or a Consumer
/// </summary>
public abstract class ReliableBase
{
    protected readonly SemaphoreSlim SemaphoreSlim = new(1, 1);
    private readonly object _lock = new();
    protected ReliableEntityStatus _status = ReliableEntityStatus.Initialization;

    protected void UpdateStatus(ReliableEntityStatus status)
    {
        lock (_lock)
        {
            _status = status;
        }
    }

    protected bool CompareStatus(ReliableEntityStatus toTest)
    {
        lock (_lock)
        {
            return _status == toTest;
        }
    }

    protected bool IsValidStatus()
    {
        lock (_lock)
        {
            return _status is ReliableEntityStatus.Open or ReliableEntityStatus.Reconnecting
                or ReliableEntityStatus.Initialization;
        }
    }

    protected abstract ILogger BaseLogger { get; }
    private IReconnectStrategy _reconnectStrategy;
    private IReconnectStrategy _resourceAvailableReconnectStrategy;

    internal async Task Init(IReconnectStrategy reconnectStrategy,
        IReconnectStrategy resourceAvailableReconnectStrategy)
    {
        _reconnectStrategy = reconnectStrategy;
        _resourceAvailableReconnectStrategy = resourceAvailableReconnectStrategy;
        await Init(true).ConfigureAwait(false);
    }

    private async Task MaybeInit(bool boot)
    {
        var reconnect = false;
        try
        {
            await CreateNewEntity(boot).ConfigureAwait(false);
            // if the createNewEntity is successful we can set the status to Open
            // else there are two ways:
            // - the exception is a known exception and the client will try to reconnect
            // - the exception is not a known exception and the client will throw the exception
            UpdateStatus(ReliableEntityStatus.Open);
        }
        catch (Exception e)
        {
            if (boot)
            {
                // if it is the first boot we don't need to reconnect
                UpdateStatus(ReliableEntityStatus.Closed);
                throw;
            }

            reconnect = ClientExceptions.IsAKnownException(e);

            LogException(e);
            if (!reconnect)
            {
                // We consider the client as closed
                // since the exception is raised to the caller
                UpdateStatus(ReliableEntityStatus.Closed);
                throw;
            }
        }

        if (reconnect)
        {
            await MaybeReconnect().ConfigureAwait(false);
        }
    }

    // <summary>
    /// Init the reliable client
    /// <param name="boot"> If it is the First boot for the reliable P/C </param>
    // </summary>
    private async Task Init(bool boot)
    {
        if (!boot && !IsValidStatus())
        {
            BaseLogger.LogInformation("{Identity} is already closed", ToString());
            return;
        }

        // each time that the client is initialized, we need to reset the status
        // if we hare here it means that the entity is not open for some reason like:
        // first time initialization or reconnect due of a IsAKnownException
        UpdateStatus(ReliableEntityStatus.Initialization);

        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            await MaybeInit(boot).ConfigureAwait(false);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Init the a new Entity (Producer/Consumer)
    /// <param name="boot"> If it is the First boot for the reliable P/C </param>
    /// Called by Init method
    /// </summary>
    internal abstract Task CreateNewEntity(bool boot);

    protected async Task<bool> CheckIfStreamIsAvailable(string stream, StreamSystem system)
    {

        await Task.Delay(Consts.RandomMid()).ConfigureAwait(false);
        var exists = false;
        var tryAgain = true;
        while (tryAgain)
        {
            try
            {
                exists = await system.StreamExists(stream).ConfigureAwait(false);
                await _resourceAvailableReconnectStrategy.WhenConnected(stream).ConfigureAwait(false);
                break;
            }
            catch (Exception e)
            {
                tryAgain = await _resourceAvailableReconnectStrategy
                    .WhenDisconnected($"Stream {stream} for {ToString()}. Error: {e.Message} ").ConfigureAwait(false);
            }
        }

        if (!exists)
        {
            // In this case the stream doesn't exist anymore
            // the  Entity is just closed.
            BaseLogger.LogInformation(
                "Meta data update stream: {StreamIdentifier}. The stream doesn't exist anymore {Identity} will be closed",
                stream,
                ToString()
            );
        }

        return exists;
    }

    // <summary>
    /// Try to reconnect to the broker
    /// Based on the retry strategy
// </summary>
    protected async Task MaybeReconnect()
    {
        var reconnect = await _reconnectStrategy.WhenDisconnected(ToString()).ConfigureAwait(false);
        if (!reconnect)
        {
            UpdateStatus(ReliableEntityStatus.Closed);
            return;
        }

        switch (IsOpen())
        {
            case true:
                await TryToReconnect().ConfigureAwait(false);
                break;
            case false:
                if (CompareStatus(ReliableEntityStatus.Reconnecting))
                {
                    BaseLogger.LogInformation("{Identity} is in Reconnecting", ToString());
                }

                break;
        }
    }

    /// <summary>
    ///  Try to reconnect to the broker
    /// </summary>
    private async Task TryToReconnect()
    {
        UpdateStatus(ReliableEntityStatus.Reconnecting);
        await MaybeInit(false).ConfigureAwait(false);
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
    private void LogException(Exception exception)
    {
        const string KnownExceptionTemplate = "{Identity} trying to reconnect due to exception {Err}";
        const string UnknownExceptionTemplate = "{Identity} received an exception during initialization";
        if (ClientExceptions.IsAKnownException(exception))
        {
            BaseLogger.LogError(KnownExceptionTemplate, ToString(), exception.Message);
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

    internal async Task OnEntityClosed(StreamSystem system, string stream)
    {
        var streamExists = false;
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            streamExists = await CheckIfStreamIsAvailable(stream, system)
                .ConfigureAwait(false);
            if (streamExists)
            {
                await MaybeReconnect().ConfigureAwait(false);
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }

        if (!streamExists)
        {
            await Close().ConfigureAwait(false);
        }
    }

    // <summary>
    /// Close the Reliable(Producer/Consumer) instance.
    // </summary>
    public abstract Task Close();

    public bool IsOpen()
    {
        lock (_lock)
        {
            return _status is ReliableEntityStatus.Open or ReliableEntityStatus.Reconnecting
                or ReliableEntityStatus.Initialization;
        }
    }
}
