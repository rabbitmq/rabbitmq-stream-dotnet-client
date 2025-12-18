// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using RabbitMQ.Stream.Client.Metrics;
using Xunit;

namespace Tests
{
    public class MetricUnitTests
    {
        private const string MeterName = "RabbitMQ.Stream.Client";

        [Fact]
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
    }
}
