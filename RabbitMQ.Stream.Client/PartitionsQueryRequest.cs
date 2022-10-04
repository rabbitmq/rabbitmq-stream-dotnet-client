// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client;

public struct PartitionsQueryRequest : ICommand
{
    private const ushort key = 0x0019;
    private readonly uint _correlationId;
    private readonly string _superStream;
    public PartitionsQueryRequest(uint correlationId, string superStream)
    {
        _correlationId = correlationId;
        _superStream = superStream;
    }

    public int SizeNeeded => 2 +
                             2 +
                             4 +
                             WireFormatting.StringSize(_superStream);

    public int Write(Span<byte> span)
    {
        var command = (ICommand)this;
        var offset = WireFormatting.WriteUInt16(span, key);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
        offset += WireFormatting.WriteUInt32(span.Slice(offset), _correlationId);
        offset += WireFormatting.WriteString(span.Slice(offset), _superStream);
        return offset;
    }
}
