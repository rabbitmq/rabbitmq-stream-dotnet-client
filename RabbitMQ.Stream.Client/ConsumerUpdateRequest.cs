// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client;

public readonly struct ConsumerUpdateRequest : ICommand
{
    private const ushort Key = 0x801a;
    private readonly uint _correlationId;

    public ConsumerUpdateRequest(uint correlationId, IOffsetType offsetSpecification)
    {
        _correlationId = correlationId;
        OffsetSpecification = offsetSpecification;
    }

    public int SizeNeeded
    {
        get
        {
            var size = 2 + 2 + 4 + 2;
            size += OffsetSpecification.Size;
            return size;
        }
    }

    public int Write(Span<byte> span)
    {
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
        offset += WireFormatting.WriteUInt32(span.Slice(offset), _correlationId);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), (ushort)ResponseCode.Ok);
        offset += OffsetSpecification.Write(span.Slice(offset));
        return offset;
    }

    public IOffsetType OffsetSpecification { get; }
}
