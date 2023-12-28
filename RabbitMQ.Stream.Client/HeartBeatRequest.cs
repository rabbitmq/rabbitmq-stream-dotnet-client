// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.Stream.Client;

internal readonly struct HeartBeatRequest : ICommand
{
    private const ushort Key = 23;

    public int SizeNeeded => 4;

    public int Write(Span<byte> span)
    {
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
        return offset;
    }
}
