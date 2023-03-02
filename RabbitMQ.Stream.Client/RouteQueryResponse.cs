// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client;

public struct RouteQueryResponse : ICommand
{
    public const ushort Key = 0x0018;

    public RouteQueryResponse(uint correlationId, ResponseCode responseCode, List<string> streams)
    {
        Streams = streams;
        ResponseCode = responseCode;
        CorrelationId = correlationId;
    }

    public List<string> Streams { get; }
    public int SizeNeeded => throw new NotImplementedException();
    public int Write(IBufferWriter<byte> writer) => throw new NotImplementedException();

    public uint CorrelationId { get; }
    public ResponseCode ResponseCode { get; }

    internal static int Read(ReadOnlySequence<byte> frame, out RouteQueryResponse command)
    {
        var offset = WireFormatting.ReadUInt16(frame, out _);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlationId);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numOfStreams);

        var streams = new List<string>();
        for (var i = 0; i < numOfStreams; i++)
        {
            offset += WireFormatting.ReadString(frame.Slice(offset), out var stream);
            streams.Add(stream);
        }

        command = new RouteQueryResponse(correlationId, (ResponseCode)responseCode, streams);
        return offset;
    }
}
