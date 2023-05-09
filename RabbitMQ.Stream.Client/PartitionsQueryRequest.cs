// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client;

internal struct PartitionsQueryRequest : ICommand
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

    public int Write(IBufferWriter<byte> writer)
    {
        var span = writer.GetSpan(SizeNeeded);
        var command = (ICommand)this;
        var offset = WireFormatting.WriteUInt16(span, key);
        offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
        offset += WireFormatting.WriteUInt32(span[offset..], _correlationId);
        offset += WireFormatting.WriteString(span[offset..], _superStream);
        writer.Advance(offset);
        return offset;
    }
}
