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

    private readonly Func<ValueTask<bool>> _sendHeartbeatFunc;
    private readonly Func<string, Task<CloseResponse>> _close;
    private readonly int _heartbeat;

    public HeartBeatHandler(Func<ValueTask<bool>> sendHeartbeatFunc,
        Func<string, Task<CloseResponse>> close,
        int heartbeat)
    {
        _sendHeartbeatFunc = sendHeartbeatFunc;
        _close = close;
        _heartbeat = heartbeat;

        // the heartbeat is disabled when zero
        // so all the timer won't be enabled

        // this is what the user can configure 
        // ex:  var config = new StreamSystemConfig()
        // {
        //     Heartbeat = TimeSpan.FromSeconds(5),
        // }
        if (heartbeat > 0)
        {
            _timer.Enabled = false;
            _timer.Interval = heartbeat * 1000;
            _timer.Elapsed += TimerElapsed;
        }
    }

    private void TimerElapsed(object sender, System.Timers.ElapsedEventArgs e)
    {
        _ = PerformHeartBeatAsync();
    }

    private async Task PerformHeartBeatAsync()
    {
        var f = _sendHeartbeatFunc();
        await f.AsTask().WaitAsync(TimeSpan.FromMilliseconds(1000));

        var seconds = (DateTime.Now - _lastUpdate).TotalSeconds;
        if (seconds < _heartbeat)
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
        await _close($"Too many Heartbeat missed: {_missedHeartbeat}. " +
              $"Client connection will be closed");
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

    internal void Start()
    {
        _timer.Enabled = true;
    }

    internal bool IsActive()
    {
        return _timer.Enabled;
    }
}
