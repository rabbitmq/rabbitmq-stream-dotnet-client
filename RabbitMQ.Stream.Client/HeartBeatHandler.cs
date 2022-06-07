// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading.Tasks;
using System.Timers;

namespace RabbitMQ.Stream.Client;

public class HeartBeatHandler
{
    private readonly Timer _timer = new();
    internal const ushort Key = 23;
    private DateTime _lastUpdate = DateTime.Now;
    private short _missedHeartbeat;

    public HeartBeatHandler(Func<ValueTask<bool>> sendHeartbeatFunc, uint heartbeat)
    {
        _timer.Enabled = true;
        _timer.Interval = 20 * 1000;
        _timer.Elapsed += (sender, args) =>
        {
            sendHeartbeatFunc();
            var seconds = (DateTime.Now - _lastUpdate).TotalSeconds;
            if (!(seconds > heartbeat))
            {
                return;
            }

            _missedHeartbeat++;
            LogEventSource.Log.LogWarning($"Heartbeat missed: {_missedHeartbeat}");
            if (_missedHeartbeat > 3)
            {
                LogEventSource.Log.LogWarning($"Too many Heartbeat missed: {_missedHeartbeat}");
            }
        };
    }

    internal void UpdateHeartBeat()
    {
        _lastUpdate = DateTime.Now;
        _missedHeartbeat = 0;
    }

    internal void Close()
    {
        _timer.Close();
    }
}
