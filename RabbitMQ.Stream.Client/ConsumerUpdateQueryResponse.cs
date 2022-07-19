// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client;

public readonly struct ConsumerUpdateQueryResponse : ICommand
{

    public const ushort Key = 0x001A;
    private readonly uint correlationId;
    private readonly byte subscriptionId;
    private readonly byte active;

    private ConsumerUpdateQueryResponse(uint correlationId, byte subscriptionId, byte active)
    {
        this.correlationId = correlationId;
        this.subscriptionId = subscriptionId;
        this.active = active;
    }

    public uint CorrelationId => correlationId;
    public byte SubscriptionId => subscriptionId;

    public bool IsActive => active == 1;
    public int SizeNeeded => throw new NotImplementedException();

    public int Write(Span<byte> span)
    {
        throw new NotImplementedException();
    }

    internal static int Read(ReadOnlySequence<byte> frame, out ConsumerUpdateQueryResponse command)
    {
        var offset = WireFormatting.ReadUInt16(frame, out _);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
        offset += WireFormatting.ReadByte(frame.Slice(offset), out var subscriptionId);
        offset += WireFormatting.ReadByte(frame.Slice(offset), out var active);
        command = new ConsumerUpdateQueryResponse(correlation, subscriptionId, active);
        return offset;
    }
}
