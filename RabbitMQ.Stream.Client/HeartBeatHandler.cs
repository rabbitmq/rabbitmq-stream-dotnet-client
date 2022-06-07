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

    public HeartBeatHandler(Func<ValueTask<bool>> sendHeartbeatFunc,
        Func<string, Task<CloseResponse>> close,
        uint heartbeat, double timerIntervalSeconds = 20)
    {
        // the heartbeat is disabled when zero
        // so all the timer won't be enabled
        if (heartbeat > 0)
        {
            _timer.Enabled = true;
            _timer.Interval = timerIntervalSeconds * 1000;
            _timer.Elapsed += (_, _) =>
            {
                sendHeartbeatFunc();
                var seconds = (DateTime.Now - _lastUpdate).TotalSeconds;
                if (!(seconds > heartbeat))
                {
                    return;
                }

                // missed the Heart beat 
                _missedHeartbeat++;
                LogEventSource.Log.LogWarning($"Heartbeat missed: {_missedHeartbeat}");
                if (_missedHeartbeat <= 3)
                {
                    return;
                }

                // When the client does not receive the Heartbeat for three times the 
                // client will be closed
                LogEventSource.Log.LogWarning($"Too many Heartbeat missed: {_missedHeartbeat}");
                Close();
                close($"Too many Heartbeat missed: {_missedHeartbeat}. " +
                      $"Client connection will be closed");
            };
        }
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

    internal bool IsActive()
    {
        return _timer.Enabled;
    }
}
