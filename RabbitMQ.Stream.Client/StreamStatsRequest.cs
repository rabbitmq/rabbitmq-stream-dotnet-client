// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;

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

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            return offset;
        }
    }
}
