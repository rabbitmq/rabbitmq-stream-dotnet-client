// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client;

public struct CommandVersionsResponse : ICommand
{
    internal const ushort Key = 0x001b;

    private CommandVersionsResponse(uint correlationId, ResponseCode responseCode, List<ICommandVersions> commands)
    {
        CorrelationId = correlationId;
        ResponseCode = responseCode;
        Commands = commands;
    }

    public int SizeNeeded { get => throw new NotImplementedException(); }
    public int Write(Span<byte> span) => throw new NotImplementedException();

    public uint CorrelationId { get; }
    public ResponseCode ResponseCode { get; }

    public List<ICommandVersions> Commands { get; }

    internal static int Read(ReadOnlySequence<byte> frame, out CommandVersionsResponse command)
    {
        var offset = WireFormatting.ReadUInt16(frame, out _);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var commandLen);
        var commands = new List<ICommandVersions>();

        for (var i = 0; i < commandLen; i++)
        {
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var commandKey);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var minVersion);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var maxVersion);
            commands.Add(new CommandVersions(commandKey, minVersion, maxVersion));
        }

        command = new CommandVersionsResponse(correlation, (ResponseCode)responseCode, commands);
        return offset;
    }
}
