// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct UnsubscribeResponse : ICommand
    {
        public const ushort Key = 12;
        private readonly uint correlationId;
        private readonly ResponseCode responseCode;

        private UnsubscribeResponse(uint correlationId, ResponseCode responseCode)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => responseCode;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out UnsubscribeResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            command = new UnsubscribeResponse(correlation, (ResponseCode)responseCode);
            return offset;
        }
    }

    public readonly struct UnsubscribeRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly byte subscriptionId;
        public const ushort Key = 12;

        public UnsubscribeRequest(uint correlationId, byte subscriptionId)
        {
            this.correlationId = correlationId;
            this.subscriptionId = subscriptionId;
        }

        public int SizeNeeded => 9;

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteByte(span.Slice(offset), subscriptionId);
            return offset;
        }
    }
}
