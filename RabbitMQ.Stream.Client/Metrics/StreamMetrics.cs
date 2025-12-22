// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace RabbitMQ.Stream.Client.Metrics
{
    public static class StreamMetricsConstants
    {
        public const string Name = "RabbitMQ.Stream.Client";
    }

    internal static class StreamMetrics
    {
        private static readonly Meter s_meter = new Meter(StreamMetricsConstants.Name);

        private static readonly object s_lock = new();

        private static UpDownCounter<long> Connections { get; } = s_meter.CreateUpDownCounter<long>(
            "rabbitmq.stream.connections",
            description: "Number of active connections");
        private static Counter<long> PublishedMessages { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.published",
            description: "Number of messages published");
        private static Counter<long> ConfirmedMessages { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.confirmed",
            description: "Number of messages confirmed");
        private static Counter<long> ErroredMessages { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.errored",
            description: "Number of messages errored");
        private static Counter<long> Chunks { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.chunk",
            description: "Number of chunks received");
        private static Histogram<long> ChunkSize { get; } = s_meter.CreateHistogram<long>(
            "rabbitmq.stream.chunk_size",
            unit: "{entries}",
            description: "Number of entries in a chunk");
        private static Counter<long> ConsumedMessages { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.consumed",
            description: "Number of messages consumed");
        private static Counter<long> WrittenBytesCount { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.written_bytes",
            unit: "By",
            description: "Number of bytes written");
        private static Counter<long> ReadBytesCount { get; } = s_meter.CreateCounter<long>(
            "rabbitmq.stream.read_bytes",
            unit: "By",
            description: "Number of bytes read");
        private static UpDownCounter<long> OutstandingPublishConfirm { get; } = s_meter.CreateUpDownCounter<long>(
            "rabbitmq.stream.outstanding_publish_confirm",
            description: "Number of messages awaiting confirmation");

        internal static void ConnectionOpened()
        {
            lock (s_lock)
            {
                Connections.Add(1);
            }
        }

        internal static void ConnectionClosed()
        {
            lock (s_lock)
            {
                Connections.Add(-1);
            }
        }

        internal static void Published(long count, string stream)
        {
            lock (s_lock)
            {
                PublishedMessages.Add(count, new KeyValuePair<string, object>("stream", stream));
            }
        }

        internal static void Confirmed(long count, string stream)
        {
            lock (s_lock)
            {
                ConfirmedMessages.Add(count, new KeyValuePair<string, object>("stream", stream));
            }
        }

        internal static void Errored(long count, string stream)
        {
            lock (s_lock)
            {
                ErroredMessages.Add(count, new KeyValuePair<string, object>("stream", stream));
            }
        }

        internal static void ChunkReceived(long entries)
        {
            lock (s_lock)
            {
                Chunks.Add(1);
                ChunkSize.Record(entries);
            }
        }

        internal static void Consumed(long count, string stream)
        {
            lock (s_lock)
            {
                ConsumedMessages.Add(count, new KeyValuePair<string, object>("stream", stream));
            }
        }

        internal static void WrittenBytes(long bytes)
        {
            lock (s_lock)
            {
                WrittenBytesCount.Add(bytes);
            }
        }

        internal static void ReadBytes(long bytes)
        {
            lock (s_lock)
            {
                ReadBytesCount.Add(bytes);
            }
        }

        internal static void OutstandingConfirmInc(long count, string stream)
        {
            lock (s_lock)
            {
                OutstandingPublishConfirm.Add(count, new KeyValuePair<string, object>("stream", stream));
            }
        }

        internal static void OutstandingConfirmDec(long count, string stream)
        {
            lock (s_lock)
            {
                OutstandingPublishConfirm.Add(-count, new KeyValuePair<string, object>("stream", stream));
            }
        }
    }
}
