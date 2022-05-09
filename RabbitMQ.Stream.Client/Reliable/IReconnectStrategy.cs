// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;

namespace RabbitMQ.Stream.Client.Reliable;

public interface IReconnectStrategy
{
    void WhenDisconnected(out bool reconnect);
    void WhenConnected();
}

internal class BackOffReconnectStrategy : IReconnectStrategy
{
    private int Tentatives { get; set; } = 1;

    public void WhenDisconnected(out bool reconnect)
    {
        Tentatives <<= 1;
        LogEventSource.Log.LogInformation(
            $"TCP disconnected, check if reconnection needed in {Tentatives * 100} ms.");
        Thread.Sleep(TimeSpan.FromMilliseconds(Tentatives * 100));
        reconnect = true;
    }

    public void WhenConnected()
    {
        Tentatives = 1;
    }
}
