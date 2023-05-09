// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct SaslHandshakeRequest : ICommand
    {
        private readonly uint correlationId;
        public const ushort Key = 18;

        public SaslHandshakeRequest(uint correlationId)
        {
            this.correlationId = correlationId;
        }

        public int SizeNeeded => 8;

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            writer.Advance(offset);
            return offset;
        }
    }
}
