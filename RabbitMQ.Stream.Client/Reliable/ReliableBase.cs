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
    /// <summary>
    /// The interface to reconnect the entity to the server.
    /// By default it uses a BackOff pattern. See <see cref="BackOffReconnectStrategy"/>
    /// </summary>
    public IReconnectStrategy ReconnectStrategy { get; set; }

    /// <summary>
    /// The interface to check if the resource is available.
    /// A stream could be not fully ready during the restarting.
    /// By default it uses a BackOff pattern. See <see cref="ResourceAvailableBackOffReconnectStrategy"/>
    /// </summary>
    public IReconnectStrategy ResourceAvailableReconnectStrategy { get; set; }

    /// <summary>
    /// The Identifier does not have any effect on the server.
    /// It is used to identify the entity in the logs and on the UI (only for the consumer)
    /// It is possible to retrieve the entity info using the Info.Identifier method form the
    /// Producer/Consumer instances.
    /// </summary>
    public string Identifier { get; set; }

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
/// The ReliableEntityStatus is used to check the status of the ReliableEntity.
/// </summary>
public enum ReliableEntityStatus
{
    Initialization,// the entity is initializing
    Open, // the entity is open and ready to use
    Reconnecting,// the entity is reconnecting but still can be used
    Closed,// the entity is closed and cannot be used anymore
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

    protected static async Task RandomWait()
    {
        await Task.Delay(Consts.RandomMid()).ConfigureAwait(false);
    }

    protected void UpdateStatus(ReliableEntityStatus status)
    {
        lock (_lock)
        {
            _status = status;
        }
    }

    private bool CompareStatus(ReliableEntityStatus toTest)
    {
        lock (_lock)
        {
            return _status == toTest;
        }
    }

    private bool IsValidStatus()
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
                BaseLogger.LogError("{Identity} Error during the first boot {EMessage}",
                    ToString(), e.Message);
                // if it is the first boot we don't need to reconnect
                UpdateStatus(ReliableEntityStatus.Closed);
                throw;
            }

            reconnect = true;
            LogException(e);
        }

        if (reconnect)
            await MaybeReconnect().ConfigureAwait(false);
    }

    // <summary>
    /// Init the reliable client
    /// <param name="boot"> If it is the First boot for the reliable P/C </param>
    // </summary>
    private async Task Init(bool boot)
    {
        if (!boot && !IsValidStatus())
        {
            BaseLogger.LogDebug("{Identity} is already closed. The init will be skipped", ToString());
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
    protected abstract Task CreateNewEntity(bool boot);

    /// <summary>
    /// When the clients receives a meta data update, it doesn't know
    /// If the stream exists or not. It just knows that the stream topology has changed.
    /// the method CheckIfStreamIsAvailable checks if the stream exists
    /// and if the leader is available.
    /// </summary>
    /// <param name="stream">stream name</param>
    /// <param name="system">stream system</param>
    /// <returns></returns>
    private async Task<bool> CheckIfStreamIsAvailable(string stream, StreamSystem system)
    {
        await Task.Delay(Consts.RandomMid()).ConfigureAwait(false);
        var exists = false;
        var tryAgain = true;
        while (tryAgain)
        {
            try
            {
                exists = await system.StreamExists(stream).ConfigureAwait(false);
                var available = exists ? "available" : "not available";
                if (exists)
                {
                    // It is not enough to check if the stream exists
                    // we need to check if the stream has the leader
                    var streamInfo = await system.StreamInfo(stream).ConfigureAwait(false);
                    ClientExceptions.CheckLeader(streamInfo);
                    available += " and has a valid leader";
                }

                await _resourceAvailableReconnectStrategy.WhenConnected($"{stream} for {ToString()} is {available}")
                    .ConfigureAwait(false);
                break;
            }
            catch (Exception e)
            {
                tryAgain = await _resourceAvailableReconnectStrategy
                    .WhenDisconnected($"Stream {stream} for {ToString()}. Error: {e.Message} ").ConfigureAwait(false);
            }
        }

        if (exists)
            return true;
        // In this case the stream doesn't exist anymore or it failed to check if the stream exists
        // too many tentatives for the reconnection strategy
        // the  Entity is just closed.
        var msg = tryAgain ? "The stream doesn't exist anymore" : "Failed to check if the stream exists";

        BaseLogger.LogInformation(
            "Meta data update stream: {StreamIdentifier}. {Msg} {Identity} will be closed",
            stream, msg,
            ToString()
        );

        return false;
    }

    // <summary>
    /// Try to reconnect to the broker
    /// Based on the retry strategy
    // </summary>
    private async Task MaybeReconnect()
    {
        var reconnect = await _reconnectStrategy.WhenDisconnected(ToString()).ConfigureAwait(false);
        if (!reconnect)
        {
            BaseLogger.LogDebug("{Identity} is closed due of reconnect strategy", ToString());
            UpdateStatus(ReliableEntityStatus.Closed);
            return;
        }

        switch (IsOpen())
        {
            case true:
                UpdateStatus(ReliableEntityStatus.Reconnecting);
                await MaybeInit(false).ConfigureAwait(false);
                break;
            case false:
                if (CompareStatus(ReliableEntityStatus.Reconnecting))
                {
                    BaseLogger.LogDebug("{Identity} is in Reconnecting", ToString());
                }

                break;
        }
    }

    private async Task MaybeReconnectPartition(StreamInfo streamInfo, string info,
        Func<StreamInfo, Task> reconnectPartitionFunc)
    {
        var reconnect = await _reconnectStrategy
            .WhenDisconnected($"Super Stream partition: {streamInfo.Stream} for {info}").ConfigureAwait(false);

        if (!reconnect)
        {
            BaseLogger.LogDebug("{Identity} partition is closed due of reconnect strategy", ToString());
            UpdateStatus(ReliableEntityStatus.Closed);
            return;
        }

        try
        {
            UpdateStatus(ReliableEntityStatus.Reconnecting);
            await reconnectPartitionFunc(streamInfo).ConfigureAwait(false);
            UpdateStatus(ReliableEntityStatus.Open);
            await _reconnectStrategy.WhenConnected(
                $"Super Stream partition: {streamInfo.Stream} for {info}").ConfigureAwait(false);
        }
        catch (Exception e)
        {
            LogException(e);
            await MaybeReconnectPartition(streamInfo, info, reconnectPartitionFunc).ConfigureAwait(false);
        }
    }

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

    /// <summary>
    /// Handle the partition reconnection in case of super stream entity
    /// </summary>
    /// <param name="system">Stream System</param>
    /// <param name="stream">Partition Stream</param>
    /// <param name="reconnectPartitionFunc">Function to reconnect the partition</param>
    internal async Task OnEntityClosed(StreamSystem system, string stream,
        Func<StreamInfo, Task> reconnectPartitionFunc)
    {
        var streamExists = false;
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            streamExists = await CheckIfStreamIsAvailable(stream, system)
                .ConfigureAwait(false);
            if (streamExists)
            {
                var streamInfo = await system.StreamInfo(stream).ConfigureAwait(false);
                await MaybeReconnectPartition(streamInfo, ToString(), reconnectPartitionFunc).ConfigureAwait(false);
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

    /// <summary>
    /// Handle the regular stream reconnection 
    /// </summary>
    /// <param name="system">Stream system</param>
    /// <param name="stream">Stream</param>
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
