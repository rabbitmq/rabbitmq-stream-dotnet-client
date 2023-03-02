// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Buffers;

namespace RabbitMQ.Stream.Client;

public readonly struct CommandVersionsRequest : ICommand
{
    private const ushort Key = 0x001b;
    private readonly uint _correlationId;
    private readonly ICommandVersions[] _commands = { new PublishFilter() };

    public CommandVersionsRequest(uint correlationId)
    {
        _correlationId = correlationId;
    }

    public int SizeNeeded
    {
        get
        {
            var size = 2 + 2 + 4
                       + 4 + // _commands.Length
                       _commands.Length * (2 + 2 + 2);
            return size;
        }
    }

    public int Write(IBufferWriter<byte> writer)
    {
        var span = writer.GetSpan(SizeNeeded);
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
        offset += WireFormatting.WriteUInt32(span[offset..], _correlationId);
        offset += WireFormatting.WriteInt32(span[offset..], _commands.Length);

        foreach (var iCommandVersions in _commands)
        {
            offset += WireFormatting.WriteUInt16(span[offset..], iCommandVersions.Command);
            offset += WireFormatting.WriteUInt16(span[offset..], iCommandVersions.MinVersion);
            offset += WireFormatting.WriteUInt16(span[offset..], iCommandVersions.MaxVersion);
        }

        writer.Advance(offset);
        return offset;
    }
}
