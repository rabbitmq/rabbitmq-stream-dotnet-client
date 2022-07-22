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
            var size = 2 + 2 + 4 + 2 + 2;
            switch (OffsetSpecification)
            {
                case OffsetTypeOffset:
                case OffsetTypeTimestamp:
                case SaCOffsetTypeOffset:
                    size += 8;
                    break;
            }

            return size;
        }
    }

    public int Write(Span<byte> span)
    {
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
        offset += WireFormatting.WriteUInt32(span.Slice(offset), _correlationId);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), (ushort)ResponseCode.Ok);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), (ushort)OffsetSpecification.OffsetType);

        switch (OffsetSpecification)
        {
            case OffsetTypeOffset typeOffset:
                offset += WireFormatting.WriteUInt64(span.Slice(offset),
                    typeOffset.OffsetValue);
                break;
            case OffsetTypeTimestamp timestamp:
                offset += WireFormatting.WriteInt64(span.Slice(offset),
                    timestamp.TimeStamp);
                break;

            case SaCOffsetTypeOffset saCTypeOffset:
                offset += WireFormatting.WriteUInt64(span.Slice(offset),
                    saCTypeOffset.OffsetValue);
                break;
        }

        return offset;
    }

    public IOffsetType OffsetSpecification { get; }
}
