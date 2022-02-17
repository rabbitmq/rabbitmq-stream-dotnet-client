// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeleteRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string stream;
        public const ushort Key = 14;

        public DeleteRequest(uint correlationId, string stream)
        {
            this.correlationId = correlationId;
            this.stream = stream;
        }

        public int SizeNeeded => 8 + WireFormatting.StringSize(stream);

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            return offset;
        }
    }

    public readonly struct DeleteResponse : ICommand
    {
        public const ushort Key = 14;
        private readonly uint correlationId;
        private readonly ushort responseCode;

        public DeleteResponse(uint correlationId, ushort responseCode)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => (ResponseCode)responseCode;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
        internal static int Read(ReadOnlySequence<byte> frame, out DeleteResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            command = new DeleteResponse(correlation, responseCode);
            return offset;
        }
    }
}
