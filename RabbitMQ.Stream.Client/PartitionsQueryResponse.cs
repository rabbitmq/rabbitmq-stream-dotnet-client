// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client;

public struct PartitionsQueryResponse : ICommand
{
    public const ushort Key = 0x0019;

    public PartitionsQueryResponse(uint correlationId, ResponseCode responseCode, string[] streams)
    {
        CorrelationId = correlationId;
        ResponseCode = responseCode;
        Streams = streams;
        SizeNeeded = 0;
    }

    public uint CorrelationId { get; }

    public ResponseCode ResponseCode { get; }

    public string[] Streams { get; }
    public int SizeNeeded { get; }

    public int Write(Span<byte> span)
    {
        throw new NotImplementedException();
    }

    internal static int Read(ReadOnlySequence<byte> frame, out PartitionsQueryResponse command)
    {
        var offset = WireFormatting.ReadUInt16(frame, out _);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
        offset += WireFormatting.ReadInt32(frame.Slice(offset), out var streamCount);
        var streams = new string[streamCount];
        for (var i = 0; i < streamCount; i++)
        {
            offset += WireFormatting.ReadString(frame.Slice(offset), out var stream);
            streams[i] = stream;
        }

        command = new PartitionsQueryResponse(correlation, (ResponseCode)responseCode, streams);
        return offset;
    }
}
