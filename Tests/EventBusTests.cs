// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Threading.Tasks;
using RabbitMQ.Stream.Client.EventBus;
using Xunit;

namespace Tests;

public class EventBusTests
{
    private class FakeEvent : IStreamEvent
    {
        public EventTypes EventType { get; } = EventTypes.Connection;
        public EventSeverity EventSeverity { get; } = EventSeverity.Info;
    }

    [Fact]
    public void SendsAndReceivesEvent()
    {
        var bus = new StreamEventsBus();
        var received = false;
        bus.Subscribe<FakeEvent>(e =>
        {
            received = true;
            return Task.CompletedTask;
        });
        bus.Publish(new FakeEvent());
        Assert.True(received);
    }
}
