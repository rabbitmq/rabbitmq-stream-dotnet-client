// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Timer = System.Timers.Timer;

namespace RabbitMQ.Stream.Client;

public class HeartBeatHandler
{
    private readonly Timer _timer = new();
    internal const ushort Key = 23;
    private DateTime _lastUpdate = DateTime.Now;
    private uint _missedHeartbeat;

    private readonly Func<ValueTask<bool>> _sendHeartbeatFunc;
    private readonly Func<string, string, Task<CloseResponse>> _close;
    private readonly int _heartbeat;
    private readonly ILogger<HeartBeatHandler> _logger;

    public HeartBeatHandler(Func<ValueTask<bool>> sendHeartbeatFunc,
        Func<string, string, Task<CloseResponse>> close,
        int heartbeat,
        ILogger<HeartBeatHandler> logger = null
    )
    {
        _sendHeartbeatFunc = sendHeartbeatFunc;
        _close = close;
        _heartbeat = heartbeat;
        _logger = logger ?? NullLogger<HeartBeatHandler>.Instance;

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
        await f.AsTask().WaitAsync(Consts.ShortWait).ConfigureAwait(false);

        var seconds = (DateTime.Now - _lastUpdate).TotalSeconds;
        if (seconds < _heartbeat)
        {
            return;
        }

        // missed the Heartbeat 
        Interlocked.Increment(ref _missedHeartbeat);
        _logger.LogWarning("Heartbeat missed: {MissedHeartbeatCounter}", _missedHeartbeat);
        if (_missedHeartbeat <= 3)
        {
            return;
        }

        // When the client does not receive the Heartbeat for three times the 
        // client will be closed
        _logger.LogCritical("Too many heartbeats missed: {MissedHeartbeatCounter}", _missedHeartbeat);
        Close();
        // The heartbeat is missed for x times the client will be closed with the reason Unexpected
        // In this way the ReliableProducer / ReliableConsumer  will be able to handle the close reason
        // and reconnect the client
        // Even it is not a perfect solution, it is a good way to handle the case to avoid to introduce breaking changes
        // we need to review all the status and the close reason on the version 2.0
        await _close($"Too many heartbeats missed: {_missedHeartbeat}. Client connection will be closed.",
            ConnectionClosedReason.MissingHeartbeat).ConfigureAwait(false);
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
