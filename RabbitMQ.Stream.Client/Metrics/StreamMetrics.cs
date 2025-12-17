// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace RabbitMQ.Stream.Client.Metrics
{
    public static class StreamMetricsConstants
    {
        public static readonly string Name = "RabbitMQ.Stream.Client";
    }

    internal static class StreamMetrics
    {
        private static readonly Meter s_meter = new Meter(StreamMetricsConstants.Name);
        
        private static readonly UpDownCounter<long> s_connections = s_meter.CreateUpDownCounter<long>(
            "rabbitmq.stream.connections",
            description: "Number of active connections");
        
        private static readonly Counter<long> s_published = s_meter.CreateCounter<long>(
            "rabbitmq.stream.published",
            description: "Number of messages published");

        private static readonly Counter<long> s_confirmed = s_meter.CreateCounter<long>(
            "rabbitmq.stream.confirmed",
            description: "Number of messages confirmed");

        private static readonly Counter<long> s_errored = s_meter.CreateCounter<long>(
            "rabbitmq.stream.errored",
            description: "Number of messages errored");

        private static readonly Counter<long> s_chunks = s_meter.CreateCounter<long>(
            "rabbitmq.stream.chunk",
            description: "Number of chunks received");

        private static readonly Histogram<long> s_chunkSize = s_meter.CreateHistogram<long>(
            "rabbitmq.stream.chunk_size",
            unit: "{entries}",
            description: "Number of entries in a chunk");

        private static readonly Counter<long> s_consumed = s_meter.CreateCounter<long>(
            "rabbitmq.stream.consumed",
            description: "Number of messages consumed");

        private static readonly Counter<long> s_writtenBytes = s_meter.CreateCounter<long>(
            "rabbitmq.stream.written_bytes",
            unit: "By",
            description: "Number of bytes written");

        private static readonly Counter<long> s_readBytes = s_meter.CreateCounter<long>(
            "rabbitmq.stream.read_bytes",
            unit: "By",
            description: "Number of bytes read");

        private static readonly UpDownCounter<long> s_outstandingPublishConfirm = s_meter.CreateUpDownCounter<long>(
            "rabbitmq.stream.outstanding_publish_confirm",
            description: "Number of messages awaiting confirmation");

        public static void ConnectionOpened() => s_connections.Add(1);
        
        public static void ConnectionClosed() => s_connections.Add(-1);
        
        public static void Published(long count, string stream) 
        {
            s_published.Add(count, new KeyValuePair<string, object>("stream", stream));
        }

        public static void Confirmed(long count, string stream)
        {
            s_confirmed.Add(count, new KeyValuePair<string, object>("stream", stream));
        }

        public static void Errored(long count, string stream)
        {
            s_errored.Add(count, new KeyValuePair<string, object>("stream", stream));
        }

        public static void ChunkReceived(long entries)
        {
            s_chunks.Add(1);
            s_chunkSize.Record(entries);
        }

        public static void Consumed(long count, string stream)
        {
            s_consumed.Add(count, new KeyValuePair<string, object>("stream", stream));
        }

        public static void WrittenBytes(long bytes) => s_writtenBytes.Add(bytes);
        
        public static void ReadBytes(long bytes) => s_readBytes.Add(bytes);

        public static void OutstandingConfirmInc(long count, string stream)
        {
            s_outstandingPublishConfirm.Add(count, new KeyValuePair<string, object>("stream", stream));
        }
        
        public static void OutstandingConfirmDec(long count, string stream)
        {
            s_outstandingPublishConfirm.Add(-count, new KeyValuePair<string, object>("stream", stream));
        }
    }
}
