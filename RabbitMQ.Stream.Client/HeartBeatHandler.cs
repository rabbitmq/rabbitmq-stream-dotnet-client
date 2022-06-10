// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;
using Timer = System.Timers.Timer;

namespace RabbitMQ.Stream.Client;

public class HeartBeatHandler
{
    private readonly Timer _timer = new();
    internal const ushort Key = 23;
    private DateTime _lastUpdate = DateTime.Now;
    private uint _missedHeartbeat;

    public HeartBeatHandler(Func<ValueTask<bool>> sendHeartbeatFunc,
        Func<string, Task<CloseResponse>> close,
        int heartbeat)
    {
        // the heartbeat is disabled when zero
        // so all the timer won't be enabled

        // this is what the user can configure 
        // ex:  var config = new StreamSystemConfig()
        // {
        //     Heartbeat = TimeSpan.FromSeconds(5),
        // }
        if (heartbeat > 0)
        {
            _timer.Enabled = true;
            _timer.Interval = heartbeat * 1000;
            _timer.Elapsed += (_, _) =>
            {
                var f = sendHeartbeatFunc();
                f.AsTask().Wait(1000);

                var seconds = (DateTime.Now - _lastUpdate).TotalSeconds;
                if (seconds < heartbeat)
                {
                    return;
                }

                // missed the Heartbeat 
                Interlocked.Increment(ref _missedHeartbeat);
                LogEventSource.Log.LogWarning($"Heartbeat missed: {_missedHeartbeat}");
                if (_missedHeartbeat <= 3)
                {
                    return;
                }

                // When the client does not receive the Heartbeat for three times the 
                // client will be closed
                LogEventSource.Log.LogError($"Too many Heartbeat missed: {_missedHeartbeat}");
                Close();
                close($"Too many Heartbeat missed: {_missedHeartbeat}. " +
                      $"Client connection will be closed");
            };
        }
    }

    internal void UpdateHeartBeat()
    {
        _lastUpdate = DateTime.Now;
        Interlocked.Exchange(ref _missedHeartbeat, 0);
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
