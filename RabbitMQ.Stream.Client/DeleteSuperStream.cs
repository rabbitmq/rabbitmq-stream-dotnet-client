// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client;

internal readonly struct DeleteSuperStreamRequest : ICommand
{
    private readonly uint _correlationId;
    private readonly string _superStream;
    private const ushort Key = 30;

    public DeleteSuperStreamRequest(uint correlationId, string superStream)
    {
        _correlationId = correlationId;
        _superStream = superStream;
    }

    public int SizeNeeded => 8 + WireFormatting.StringSize(_superStream);

    public int Write(IBufferWriter<byte> writer)
    {
        var span = writer.GetSpan(SizeNeeded);
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
        offset += WireFormatting.WriteUInt32(span[offset..], _correlationId);
        offset += WireFormatting.WriteString(span[offset..], _superStream);
        writer.Advance(offset);
        return offset;
    }
}

public readonly struct DeleteSuperStreamResponse : ICommand
{
    public const ushort Key = 30;
    private readonly uint _correlationId;
    private readonly ushort _responseCode;

    public DeleteSuperStreamResponse(uint correlationId, ushort responseCode)
    {
        _correlationId = correlationId;
        _responseCode = responseCode;
    }

    public int SizeNeeded => throw new NotImplementedException();

    public uint CorrelationId => _correlationId;

    public ResponseCode ResponseCode => (ResponseCode)_responseCode;

    public int Write(IBufferWriter<byte> writer)
    {
        throw new NotImplementedException();
    }

    internal static int Read(ReadOnlySequence<byte> frame, out DeleteSuperStreamResponse command)
    {
        var offset = WireFormatting.ReadUInt16(frame, out _);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
        command = new DeleteSuperStreamResponse(correlation, responseCode);
        return offset;
    }
}
