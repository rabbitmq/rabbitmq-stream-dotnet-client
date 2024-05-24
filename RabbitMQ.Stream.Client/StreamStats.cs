// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client;

public class StreamStats
{
    private void MaybeOffsetNotFound(long value, string offsetType)
    {
        if (value == -1)
        {
            throw new OffsetNotFoundException($"{offsetType} not found for stream {_stream}");
        }
    }

    /// <summary>
    /// The first offset in the stream.
    ///
    /// return first offset in the stream
    /// throws NoOffsetException if there is no first offset yet
    /// </summary>
    public ulong FirstOffset()
    {
        var r = _statistics.TryGetValue("first_chunk_id", out var value) ? value : -1;
        MaybeOffsetNotFound(value, "FirstOffset");
        return (ulong)r;
    }

    [Obsolete("LastOffset() is deprecated, please use CommittedChunkId instead.")]
    public ulong LastOffset()
    {
        var r = _statistics.TryGetValue("last_chunk_id", out var value) ? value : -1;
        MaybeOffsetNotFound(value, "LastOffset");
        return (ulong)r;
    }

    /// <summary>
    /// The ID (offset) of the committed chunk (block of messages) in the stream.
    ///
    /// It is the offset of the first message in the last chunk confirmed by a quorum of the stream
    /// cluster members (leader and replicas).
    ///
    /// The committed chunk ID is a good indication of what the last offset of a stream can be at a
    /// given time. The value can be stale as soon as the application reads it though, as the committed
    /// chunk ID for a stream that is published to changes all the time.
    ///
    /// return committed offset in this stream
    /// throws NoOffsetException if there is no committed chunk yet
    ///
    /// </summary>
    public ulong CommittedChunkId()
    {
        var r = _statistics.TryGetValue("committed_chunk_id", out var value) ? value : -1;
        MaybeOffsetNotFound(value, "CommittedChunkId");
        return (ulong)r;
    }

    private readonly IDictionary<string, long> _statistics;
    private readonly string _stream;

    internal StreamStats(IDictionary<string, long> statistics, string stream)
    {
        _statistics = statistics;
        _stream = stream;
    }
}
