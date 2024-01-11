// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client.Reconnect;

/// <summary>
/// IReconnectStrategy is the interface to reconnect the TCP client
/// </summary>
public interface IReconnectStrategy
{
    /// <summary>
    /// WhenDisconnected is raised when the TPC client
    /// is disconnected for some reason. 
    /// </summary>
    /// <param name="itemIdentifier">Additional connection info. Just for logging</param>
    /// <returns>if True the client will be reconnected else closed</returns>
    ValueTask<bool> WhenDisconnected(string itemIdentifier);

    /// <summary>
    /// It is raised when the TCP client is connected successfully 
    /// </summary>
    /// <param name="itemIdentifier">Additional info. Just for logging</param>
    ValueTask WhenConnected(string itemIdentifier);
}

/// <summary>
/// BackOffReconnectStrategy is the default IReconnectStrategy
/// implementation for Producer and Consumer
/// It implements a BackOff pattern.
/// </summary>
internal class BackOffReconnectStrategy : IReconnectStrategy
{
    private int Tentatives { get; set; } = 1;
    private readonly ILogger _logger;

    public BackOffReconnectStrategy(ILogger logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    // reset the tentatives after a while 
    // else the backoff will be too long
    private void MaybeResetTentatives()
    {
        if (Tentatives > 1000)
        {
            Tentatives = 1;
        }
    }

    public async ValueTask<bool> WhenDisconnected(string connectionIdentifier)
    {
        Tentatives <<= 1;
        _logger.LogInformation(
            "{ConnectionIdentifier} disconnected, check if reconnection needed in {ReconnectionDelayMs} ms",
            connectionIdentifier,
            Tentatives * 100
        );
        await Task.Delay(TimeSpan.FromMilliseconds(Tentatives * 100)).ConfigureAwait(false);
        MaybeResetTentatives();
        return true;
    }

    public ValueTask WhenConnected(string connectionIdentifier)
    {
        Tentatives = 1;
        _logger.LogInformation("{ConnectionIdentifier} connected successfully", connectionIdentifier);
        return ValueTask.CompletedTask;
    }
}

internal class ResourceAvailableBackOffReconnectStrategy : IReconnectStrategy
{
    private int Tentatives { get; set; } = 1;
    private readonly ILogger _logger;

    public ResourceAvailableBackOffReconnectStrategy(ILogger logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    // reset the tentatives after a while 
    // else the backoff will be too long
    private void MaybeResetTentatives()
    {
        if (Tentatives > 4)
        {
            Tentatives = 1;
        }
    }

    public async ValueTask<bool> WhenDisconnected(string resourceIdentifier)
    {
        Tentatives <<= 1;
        _logger.LogInformation(
            "{ConnectionIdentifier} resource not available, retry in {ReconnectionDelayMs} seconds",
            resourceIdentifier,
            Tentatives
        );
        await Task.Delay(TimeSpan.FromSeconds(Tentatives)).ConfigureAwait(false);
        MaybeResetTentatives();
        return Tentatives < 4;
    }

    public ValueTask WhenConnected(string resourceIdentifier)
    {
        Tentatives = 1;
        _logger.LogInformation("{ResourceIdentifier} is available", resourceIdentifier);
        return ValueTask.CompletedTask;
    }
}
