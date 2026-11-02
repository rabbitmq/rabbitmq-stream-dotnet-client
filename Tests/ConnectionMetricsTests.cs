// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    // Verifies that the open/closed connection metric stays balanced.
    // The metrics are static: tests run sequentially (see xunit.runner.json), so the collector
    // only sees the connections created by the test itself.
    public class ConnectionMetricsTests
    {
        private const string MeterName = "RabbitMQ.Stream.Client";
        private const string ConnectionsInstrument = "rabbitmq.stream.connections";

        [Fact]
        public async Task ConcurrentCloseRecordsConnectionClosedOnlyOnce()
        {
            using var collector = new MetricCollector<long>(null, MeterName, ConnectionsInstrument);
            var client = await Client.Create(new ClientParameters());

            // InternalClose can be reached from several threads (user Close, heartbeat, reader)
            await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Task.Run(() => client.Close("test"))));
            await Task.Delay(500);

            var snapshot = collector.GetMeasurementSnapshot();
            Assert.Equal(1, snapshot.Count(m => m.Value == 1));
            Assert.Equal(1, snapshot.Count(m => m.Value == -1));
            Assert.Equal(0, snapshot.Sum(m => m.Value));
        }

        [Fact]
        public async Task CloseCalledTwiceRecordsConnectionClosedOnlyOnce()
        {
            using var collector = new MetricCollector<long>(null, MeterName, ConnectionsInstrument);
            var client = await Client.Create(new ClientParameters());

            await client.Close("first");
            await client.Close("second");

            Assert.Equal(0, collector.GetMeasurementSnapshot().Sum(m => m.Value));
        }

        [Fact]
        public async Task DroppedSocketStillRecordsConnectionClosed()
        {
            using var collector = new MetricCollector<long>(null, MeterName, ConnectionsInstrument);
            var client = await Client.Create(new ClientParameters());

            // Simulate the socket dropping before the client runs its close logic:
            // Socket.Connected (and so Connection.IsClosed) is already false when InternalClose runs.
            var connection = typeof(Client).GetField("_connection", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(client)!;
            var socket = (Socket)typeof(Connection).GetField("socket", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(connection)!;
            socket.Close();

            // the reader notices the closed socket and closes the client
            await SystemUtils.WaitUntilAsync(() => collector.GetMeasurementSnapshot().Any(m => m.Value == -1));

            Assert.Equal(0, collector.GetMeasurementSnapshot().Sum(m => m.Value));
        }
    }
}
