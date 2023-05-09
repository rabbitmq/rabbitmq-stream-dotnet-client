// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct DeclarePublisherRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly byte publisherId;
        private readonly string publisherRef;
        private readonly string stream;
        public const ushort Key = 1;

        public DeclarePublisherRequest(uint correlationId, byte publisherId, string publisherRef, string stream)
        {
            this.correlationId = correlationId;
            this.publisherId = publisherId;
            this.publisherRef = publisherRef;
            this.stream = stream;
        }

        public int SizeNeeded => 8 + 1 + WireFormatting.StringSize(publisherRef) + WireFormatting.StringSize(stream);

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            offset += WireFormatting.WriteByte(span[offset..], publisherId);
            offset += WireFormatting.WriteString(span[offset..], publisherRef);
            offset += WireFormatting.WriteString(span[offset..], stream);
            writer.Advance(offset);
            return offset;
        }
    }
}
