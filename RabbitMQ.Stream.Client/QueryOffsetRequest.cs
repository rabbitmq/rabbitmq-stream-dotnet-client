// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct QueryOffsetRequest : ICommand
    {
        public const ushort Key = 11;
        private readonly string stream;
        private readonly uint corrId;
        private readonly string reference;

        public QueryOffsetRequest(string stream, uint corrId, string reference)
        {
            this.stream = stream;
            this.corrId = corrId;
            this.reference = reference;
        }

        public int SizeNeeded =>
            2 + 2 + 4 + WireFormatting.StringSize(reference) + WireFormatting.StringSize(stream);

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
            offset += WireFormatting.WriteUInt32(span[offset..], corrId);
            offset += WireFormatting.WriteString(span[offset..], reference);
            offset += WireFormatting.WriteString(span[offset..], stream);
            writer.Advance(offset);
            return offset;
        }
    }
}
