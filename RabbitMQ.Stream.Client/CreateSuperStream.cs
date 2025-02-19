// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace RabbitMQ.Stream.Client;

internal readonly struct CreateSuperStreamRequest : ICommand
{
    private const ushort Key = 29;
    private readonly string _superStream;
    private readonly uint _corrId;
    private readonly IDictionary<string, string> _arguments;
    private readonly List<string> _partitions;
    private readonly List<string> _bindingKeys;

    internal CreateSuperStreamRequest(uint corrId, string superStream,
        List<string> partitions, List<string> bindingKeys, IDictionary<string, string> args)
    {
        _corrId = corrId;
        _superStream = superStream;
        _partitions = partitions;
        _bindingKeys = bindingKeys;
        _arguments = args;
    }

    public int SizeNeeded
    {
        get
        {
            var size =
                       2
                       + 2
                       + 4
                       + WireFormatting.StringSize(_superStream);

            size += 4 + _partitions.Sum(WireFormatting.StringSize);
            size += 4 + _bindingKeys.Sum(WireFormatting.StringSize);
            size += 4
                    + _arguments.Sum(x => WireFormatting.StringSize(x.Key) + WireFormatting.StringSize(x.Value));
            return size;
        }
    }

    public int Write(IBufferWriter<byte> writer)
    {
        var span = writer.GetSpan(SizeNeeded);
        var command = (ICommand)this;
        var offset = WireFormatting.WriteUInt16(span, Key);
        offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
        offset += WireFormatting.WriteUInt32(span[offset..], _corrId);
        offset += WireFormatting.WriteString(span[offset..], _superStream);
        offset += WireFormatting.WriteInt32(span[offset..], _partitions.Count);
        foreach (var partition in _partitions)
        {
            offset += WireFormatting.WriteString(span[offset..], partition);
        }

        offset += WireFormatting.WriteInt32(span[offset..], _bindingKeys.Count);
        foreach (var bindingKey in _bindingKeys)
        {
            offset += WireFormatting.WriteString(span[offset..], bindingKey);
        }

        offset += WireFormatting.WriteInt32(span[offset..], _arguments.Count);
        foreach (var (key, value) in _arguments)
        {
            offset += WireFormatting.WriteString(span[offset..], key);
            offset += WireFormatting.WriteString(span[offset..], value);
        }

        writer.Advance(offset);
        return offset;
    }
}

public readonly struct CreateSuperStreamResponse : ICommand
{
    public const ushort Key = 29;
    private readonly uint _correlationId;
    private readonly ushort _responseCode;

    private CreateSuperStreamResponse(uint correlationId, ushort responseCode)
    {
        _correlationId = correlationId;
        _responseCode = responseCode;
    }

    public int SizeNeeded => throw new NotImplementedException();

    public uint CorrelationId => _correlationId;

    public ResponseCode ResponseCode => (ResponseCode)_responseCode;

    public int Write(IBufferWriter<byte> writer)
    {
        throw new NotImplementedException();
    }

    internal static int Read(ReadOnlySequence<byte> frame, out CreateSuperStreamResponse command)
    {
        var offset = WireFormatting.ReadUInt16(frame, out _);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
        offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
        offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
        command = new CreateSuperStreamResponse(correlation, responseCode);
        return offset;
    }
}
