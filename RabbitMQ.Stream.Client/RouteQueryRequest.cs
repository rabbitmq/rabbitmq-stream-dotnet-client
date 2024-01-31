// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.Stream.Client;

internal readonly struct RouteQueryRequest : ICommand
{
    public const ushort Key = 0x0018;
    private readonly string _superStream;
    private readonly string _routingKey;
    private readonly uint _corrId;

    public RouteQueryRequest(uint corrId, string superStream, string routingKey)
    {
        _corrId = corrId;
        _superStream = superStream;
        _routingKey = routingKey;
    }

    public int SizeNeeded => 2 + 2 + 4 +
                             WireFormatting.StringSize(_superStream) +
                             WireFormatting.StringSize(_routingKey);

    public int Write(Span<byte> span)
    {
        var command = (ICommand)this;
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
        offset += WireFormatting.WriteUInt32(span[offset..], _corrId);
        offset += WireFormatting.WriteString(span[offset..], _routingKey);
        offset += WireFormatting.WriteString(span[offset..], _superStream);
        return offset;
    }
}
