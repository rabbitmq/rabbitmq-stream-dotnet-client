// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslHandshakeRequest : ICommand
    {
        private readonly uint correlationId;
        public const ushort Key = 18;

        public SaslHandshakeRequest(uint correlationId)
        {
            this.correlationId = correlationId;
        }

        public int SizeNeeded => 8;

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            return offset;
        }
    }
}
