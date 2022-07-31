// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client.Reliable;

/// <summary>
/// IReconnectStrategy is the interface to reconnect the TCP client
/// </summary>
public interface IReconnectStrategy
{
    /// <summary>
    /// WhenDisconnected is raised when the TPC client
    /// is disconnected for some reason. 
    /// </summary>
    /// <param name="connectionInfo">Additional connection info. Just for logging</param>
    /// <returns>if True the client will be reconnected else closed</returns>
    bool WhenDisconnected(string connectionInfo);

    /// <summary>
    /// It is raised when the TCP client is connected successfully 
    /// </summary>
    /// <param name="connectionInfo">Additional connection info. Just for logging</param>
    void WhenConnected(string connectionInfo);
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
        _logger = logger;
    }

    public bool WhenDisconnected(string connectionInfo)
    {
        Tentatives <<= 1;
        var sleepDuration = Tentatives * 100;
        _logger?.LogInformation(
            "{ConnectionInfo} disconnected, check if reconnection needed in {SleepDuration} ms.",
            connectionInfo,
            sleepDuration
            );
        Thread.Sleep(TimeSpan.FromMilliseconds(sleepDuration));
        return true;
    }

    public void WhenConnected(string _)
    {
        Tentatives = 1;
    }
}
