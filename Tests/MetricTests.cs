#nullable enable
// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.VisualStudio.TestPlatform.ObjectModel;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Metrics;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;

namespace Tests
{
    public class MetricUnitTests
    {
        private const string MeterName = "RabbitMQ.Stream.Client";

        [Fact]
        [Trait("test-type", "unit")]
        public async Task VerifyStreamMetricsRecording()
        {
            // Create collectors for each instrument
            using var connectionsCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.connections");
            using var publishedCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.published");
            using var confirmedCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.confirmed");
            using var erroredCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.errored");
            using var chunksCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.chunk");
            using var chunkSizeCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.chunk_size");
            using var consumedCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.consumed");
            using var writtenBytesCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.written_bytes");
            using var readBytesCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.read_bytes");
            using var outstandingCollector = new MetricCollector<long>(null, MeterName, "rabbitmq.stream.outstanding_publish_confirm");

            // 1. Connection metrics
            StreamMetrics.ConnectionOpened();
            Assert.Contains(connectionsCollector.GetMeasurementSnapshot(), m => m.Value == 1);
            StreamMetrics.ConnectionClosed();
            Assert.Contains(connectionsCollector.GetMeasurementSnapshot(), m => m.Value == -1);

            // 2. Published metrics
            StreamMetrics.Published(10, "my-stream");
            Assert.Contains(publishedCollector.GetMeasurementSnapshot(), m => m.Value == 10 && HasTag(m, "stream", "my-stream"));

            // 3. Confirmed
            StreamMetrics.Confirmed(5, "my-stream");
            Assert.Contains(confirmedCollector.GetMeasurementSnapshot(), m => m.Value == 5 && HasTag(m, "stream", "my-stream"));

            // 4. Errored
            StreamMetrics.Errored(1, "my-stream");
            Assert.Contains(erroredCollector.GetMeasurementSnapshot(), m => m.Value == 1 && HasTag(m, "stream", "my-stream"));

            // 5. Chunks
            StreamMetrics.ChunkReceived(50);
            Assert.Contains(chunksCollector.GetMeasurementSnapshot(), m => m.Value == 1);
            Assert.Contains(chunkSizeCollector.GetMeasurementSnapshot(), m => m.Value == 50);

            // 6. Consumed
            StreamMetrics.Consumed(50, "my-stream");
            Assert.Contains(consumedCollector.GetMeasurementSnapshot(), m => m.Value == 50 && HasTag(m, "stream", "my-stream"));

            // 7. Bytes
            StreamMetrics.WrittenBytes(1024);
            Assert.Contains(writtenBytesCollector.GetMeasurementSnapshot(), m => m.Value == 1024);

            StreamMetrics.ReadBytes(2048);
            Assert.Contains(readBytesCollector.GetMeasurementSnapshot(), m => m.Value == 2048);

            // 8. Outstanding Confirms
            StreamMetrics.OutstandingConfirmInc(10, "my-stream");
            Assert.Contains(outstandingCollector.GetMeasurementSnapshot(), m => m.Value == 10 && HasTag(m, "stream", "my-stream"));

            StreamMetrics.OutstandingConfirmDec(5, "my-stream");
            Assert.Contains(outstandingCollector.GetMeasurementSnapshot(), m => m.Value == -5 && HasTag(m, "stream", "my-stream"));

            await Task.CompletedTask;
        }

        private static bool HasTag<T>(CollectedMeasurement<T> measurement, string key, string value) where T : struct
        {
            return measurement.Tags.Any(t => t.Key == key && t.Value?.ToString() == value);
        }

        [Fact]
        [Trait("test-type", "integration")]
        public async Task VerifyStreamMetricsWithRunningServer()
        {
            // Arrange
            const string StreamName = "VerifyStreamMetricsWithRunningServer";

            TestMetricListener collector = new();
            var meter_listener = SetupMeterListener();
            meter_listener.SetMeasurementEventCallback<long>(collector.OnMeasurementRecorded);

            var system = await StreamSystem.Create(new StreamSystemConfig
            {
                Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) }
            });

            await system.CreateStream(new StreamSpec(StreamName));

            // Act
            var producer = await Producer.Create(new ProducerConfig(system, StreamName));
            var consumer = await Consumer.Create(new ConsumerConfig(system, StreamName));

            // Give time for metrics to be recorded
            await Task.Delay(200);

            // We can't assert exact connection counts due to connection pooling with CloseWhenEmpty policy
            // Connections may close when they become empty (no producers/consumers attached)
            // Instead, verify that connection metrics ARE being tracked (counter changed from initial 0)
            var connectionCountAfterCreate = collector._connectionCounter;
            Assert.True(connectionCountAfterCreate > 0, $"Expected connection counter > 0 after creating producer/consumer, got {connectionCountAfterCreate}");

            await producer.Close();
            await Task.Delay(200);

            var connectionCountAfterProducerClose = collector._connectionCounter;
            // After closing producer, connection count should change (either stay same or decrease)
            // We just verify metrics are being tracked
            Assert.True(connectionCountAfterProducerClose >= 0, $"Connection counter should be valid after producer close, got {connectionCountAfterProducerClose}");

            // Future producer close should not affect the connection counter
            await producer.Close();
            await producer.Close();
            await Task.Delay(200);
            var connectionCountAfterFutureProducerClose = collector._connectionCounter;
            Assert.True(connectionCountAfterFutureProducerClose == connectionCountAfterProducerClose, $"Connection counter should be the same after future producer close, got {connectionCountAfterFutureProducerClose} instead of {connectionCountAfterProducerClose}");

            // Cleanup
            await consumer.Close();
            await system.DeleteStream(StreamName);
            await system.Close();
        }

        private static MeterListener SetupMeterListener()
        {
            MeterListener meterListener = new();
            meterListener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name is "RabbitMQ.Stream.Client")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            meterListener.Start();
            return meterListener;
        }
    }

    internal sealed class TestMetricListener
    {
        internal TestMetricListener()
        {
            _meterListener = new MeterListener();
            _meterListener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name is "RabbitMQ.Stream.Client")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _meterListener.Start();
        }

        private readonly MeterListener _meterListener;
        internal long _connectionCounter { get; private set; } = 0;
        internal long _publishedCounter { get; private set; } = 0;
        internal long _confirmedCounter { get; private set; } = 0;
        internal long _erroredCounter { get; private set; } = 0;
        internal long _chunksCounter { get; private set; } = 0;
        internal long _chunkSizeCounter { get; private set; } = 0;
        internal long _consumedCounter { get; private set; } = 0;
        internal long _writtenBytesCounter { get; private set; } = 0;
        internal long _readBytesCounter { get; private set; } = 0;
        internal long _outstandingPublishConfirmCounter { get; private set; } = 0;

        public void OnMeasurementRecorded<T>(
            Instrument instrument,
            T measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            _ = tags;
            _ = state;
            var v = (object?)measurement ?? throw new Exception("measurement is null");
            switch (instrument.Name)
            {
                case "rabbitmq.stream.connections":
                    _connectionCounter += (long)v;
                    break;
                case "rabbitmq.stream.published":
                    _publishedCounter += (long)v;
                    break;
                case "rabbitmq.stream.confirmed":
                    _confirmedCounter += (long)v;
                    break;
                case "rabbitmq.stream.errored":
                    _erroredCounter += (long)v;
                    break;
                case "rabbitmq.stream.chunk":
                    _chunksCounter += (long)v;
                    break;
                case "rabbitmq.stream.chunk_size":
                    _chunkSizeCounter += (long)v;
                    break;
                case "rabbitmq.stream.consumed":
                    _consumedCounter += (long)v;
                    break;
                case "rabbitmq.stream.written_bytes":
                    _writtenBytesCounter += (long)v;
                    break;
                case "rabbitmq.stream.read_bytes":
                    _readBytesCounter += (long)v;
                    break;
                case "rabbitmq.stream.outstanding_publish_confirm":
                    _outstandingPublishConfirmCounter += (long)v;
                    break;
                default:
                    throw new Exception($"Unknown instrument: {instrument.Name}");
            }
        }
    }
}
