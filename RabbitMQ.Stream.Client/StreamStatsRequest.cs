// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct StreamStatsRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string stream;
        public const ushort Key = 0x001c;

        public StreamStatsRequest(uint correlationId, string stream)
        {
            this.correlationId = correlationId;
            this.stream = stream;
        }

        public int SizeNeeded => 8 + WireFormatting.StringSize(stream);

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            offset += WireFormatting.WriteString(span[offset..], stream);
            writer.Advance(offset);
            return offset;
        }
    }
}
