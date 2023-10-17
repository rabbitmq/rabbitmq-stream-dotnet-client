// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.EventBus;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class EventBusTests
{
    private class FakeEvent : IStreamEvent
    {
        public EventTypes EventType { get; } = EventTypes.Connection;
        public EventSeverity EventSeverity { get; } = EventSeverity.Info;
    }

    private readonly ITestOutputHelper _testOutputHelper;

    public EventBusTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
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

    [Fact]
    public async Task SendsAndReceivesRawProducerEvents()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var bus = new StreamEventsBus();
        var testPassed = new TaskCompletionSource<List<IStreamEvent>>();
        var events = new List<IStreamEvent>();
        bus.Subscribe<RawProducerConnected>(async connected =>
        {
            events.Add(connected);
            await Task.CompletedTask;
        });
        bus.Subscribe<RawProducerDisconnected>(async disconnected =>
        {
            events.Add(disconnected);
            testPassed.SetResult(events);
            await Task.CompletedTask;
        });
        var rawProducer = await system.CreateRawProducer(new RawProducerConfig(stream) { Events = bus });
        SystemUtils.Wait();
        await rawProducer.Close();
        new Utils<List<IStreamEvent>>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        var connected = (RawProducerConnected)events[0];
        Assert.Equal(new IPEndPoint(IPAddress.IPv6Loopback, 5552), connected.Parameters.Endpoint);
        Assert.Equal("dotnet-stream-raw-producer", connected.Parameters.ClientProvidedName);
        Assert.Equal(typeof(RawProducerConnected), connected.GetType());
        Assert.Equal(EventTypes.Connection, connected.EventType);
        Assert.Equal(EventSeverity.Info, connected.EventSeverity);
        var disconnected = (RawProducerDisconnected)events[1];
        Assert.Equal(new IPEndPoint(IPAddress.IPv6Loopback, 5552), disconnected.Parameters.Endpoint);
        Assert.Equal("dotnet-stream-raw-producer", disconnected.Parameters.ClientProvidedName);
        Assert.Equal(typeof(RawProducerDisconnected), disconnected.GetType());
        Assert.Equal(EventTypes.Disconnection, disconnected.EventType);
        Assert.Equal(EventSeverity.Info, disconnected.EventSeverity);
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task SendsAndReceivesSuperStreamRawProducerEvents()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var testPassed = new TaskCompletionSource<List<IStreamEvent>>();
        var events = new List<IStreamEvent>();
        var bus = new StreamEventsBus();
        bus.Subscribe<RawProducerConnected>(async connected =>
        {
            _testOutputHelper.WriteLine(
                $"raw producer connected {connected.Instance.Info.ClientProvidedName} to stream {connected.Instance.Info.Stream}");
            events.Add(connected);
            await Task.CompletedTask;
        });
        bus.Subscribe<RawProducerDisconnected>(async disconnected =>
        {
            events.Add(disconnected);
            _testOutputHelper.WriteLine(
                $"raw producer disconnected {disconnected.Instance.Info.ClientProvidedName} from the stream {disconnected.Instance.Info.Stream}. Events: {events.Count}");

            if (events.Count == 6)
            {
                testPassed.SetResult(events);
            }

            await Task.CompletedTask;
        });
        var rawSuperStreamProducer =
            await system.CreateRawSuperStreamProducer(
                new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
                {
                    Events = bus,
                    Routing = message => message.Properties.MessageId.ToString(),
                    RoutingStrategyType = RoutingStrategyType.Hash
                });

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            await rawSuperStreamProducer.Send(i, message);
        }

        SystemUtils.Wait();
        await rawSuperStreamProducer.Close();
        new Utils<List<IStreamEvent>>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        for (var i = 0; i < 3; i++)
        {
            var connected = (RawProducerConnected)events[i];
            Assert.Equal(new IPEndPoint(IPAddress.IPv6Loopback, 5552), connected.Parameters.Endpoint);
            Assert.Contains(connected.Instance.Info.Stream, SystemUtils.InvoicesStreams);
            Assert.Contains(connected.Instance.Info.ClientProvidedName, $"dotnet-stream-raw-producer#invoices-{i}");
            Assert.Equal(typeof(RawProducerConnected), connected.GetType());
            Assert.Contains(connected.Instance.Info.ClientProvidedName, $"dotnet-stream-raw-producer#invoices-{i}");
            Assert.Equal(EventTypes.Connection, connected.EventType);
            Assert.Equal(EventSeverity.Info, connected.EventSeverity);
        }

        for (var i = 3; i < 6; i++)
        {
            var disconnected = (RawProducerDisconnected)events[i];
            Assert.Equal(new IPEndPoint(IPAddress.IPv6Loopback, 5552), disconnected.Parameters.Endpoint);
            Assert.Contains(disconnected.Instance.Info.Stream, SystemUtils.InvoicesStreams);
            Assert.Contains(disconnected.Instance.Info.ClientProvidedName, $"dotnet-stream-raw-producer#invoices-{i}");
            Assert.Equal(typeof(RawProducerDisconnected), disconnected.GetType());
            Assert.Equal(EventTypes.Disconnection, disconnected.EventType);
            Assert.Equal(EventSeverity.Info, disconnected.EventSeverity);
        }

        await system.Close();
    }

    [Fact]
    public async Task SendsAndReceivesProducerReconnectingEvent()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var bus = new StreamEventsBus();
        var testPassed = new TaskCompletionSource<List<IStreamEvent>>();
        var events = new List<IStreamEvent>();

        bus.Subscribe<ProducerReconnected>(async reconnected =>
        {
            events.Add(reconnected);
            if (events.Count == 2)
            {
                testPassed.SetResult(events);
            }

            await Task.CompletedTask;
        });
        var clientProvidedName = Guid.NewGuid().ToString();
        var producer = await Producer.Create(new ProducerConfig(system, stream)
        {
            Events = bus,
            ClientProvidedName = clientProvidedName
        }
        );

        SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientProvidedName).Result == 1);

        new Utils<List<IStreamEvent>>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);

        var inReconnection = (ProducerReconnected)events[0];
        Assert.Equal(typeof(ProducerReconnected), inReconnection.GetType());
        Assert.Equal(EventTypes.Reconnection, inReconnection.EventType);
        Assert.Equal(EventSeverity.Warning, inReconnection.EventSeverity);
        Assert.Equal(stream, inReconnection.Instance.Info.Stream);
        Assert.True(inReconnection.IsReconnection);

        var reconnected = (ProducerReconnected)events[1];
        Assert.Equal(typeof(ProducerReconnected), reconnected.GetType());
        Assert.Equal(EventTypes.Reconnection, reconnected.EventType);
        Assert.Equal(EventSeverity.Info, reconnected.EventSeverity);
        Assert.False(reconnected.IsReconnection);

        await SystemUtils.CleanUpStreamSystem(system, stream).ConfigureAwait(false);
    }
}
