﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Buffers;

namespace RabbitMQ.Stream.Client;

internal readonly struct HeartBeatRequest : ICommand
{
    private const ushort Key = 23;

    public int SizeNeeded => 4;

    public int Write(IBufferWriter<byte> writer)
    {
        var span = writer.GetSpan(SizeNeeded);
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
        writer.Advance(offset);
        return offset;
    }
}
