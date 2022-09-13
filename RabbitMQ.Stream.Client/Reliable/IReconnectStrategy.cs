// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading.Tasks;

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
    ValueTask<bool> WhenDisconnected(string connectionInfo);

    /// <summary>
    /// It is raised when the TCP client is connected successfully 
    /// </summary>
    /// <param name="connectionInfo">Additional connection info. Just for logging</param>
    ValueTask WhenConnected(string connectionInfo);
}

/// <summary>
/// BackOffReconnectStrategy is the default IReconnectStrategy
/// implementation for Producer and Consumer
/// It implements a BackOff pattern.
/// </summary>
internal class BackOffReconnectStrategy : IReconnectStrategy
{
    private int Tentatives { get; set; } = 1;
    // reset the tentatives after a while 
    // else the backoff will be too long
    private void MaybeResetTentatives()
    {
        if (Tentatives > 1000)
        {
            Tentatives = 1;
        }
    }

    public async ValueTask<bool> WhenDisconnected(string connectionInfo)
    {
        Tentatives <<= 1;
        LogEventSource.Log.LogInformation(
            $"{connectionInfo} disconnected, check if reconnection needed in {Tentatives * 100} ms.");
        await Task.Delay(TimeSpan.FromMilliseconds(Tentatives * 100));
        MaybeResetTentatives();
        return true;
    }

    public ValueTask WhenConnected(string connectionInfo)
    {
        Tentatives = 1;
        return ValueTask.CompletedTask;
    }
}
