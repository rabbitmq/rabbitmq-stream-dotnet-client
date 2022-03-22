// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslAuthenticateRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string mechanism;
        private readonly byte[] data;
        public const ushort Key = 19;

        public SaslAuthenticateRequest(uint correlationId, string mechanism, byte[] data)
        {
            this.correlationId = correlationId;
            this.mechanism = mechanism;
            this.data = data;
        }

        public int SizeNeeded => 4 + 4 + WireFormatting.StringSize(mechanism) + 4 + data.Length;

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), mechanism);
            offset += WireFormatting.WriteBytes(span.Slice(offset), new ReadOnlySequence<byte>(data));
            return offset;
        }
    }
}
