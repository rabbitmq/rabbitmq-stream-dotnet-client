﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct QueryPublisherRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string publisherRef;
        private readonly string stream;
        public const ushort Key = 5;

        public QueryPublisherRequest(uint correlationId, string publisherRef, string stream)
        {
            this.correlationId = correlationId;
            this.publisherRef = publisherRef;
            this.stream = stream;
        }
        public int SizeNeeded => 8 + WireFormatting.StringSize(publisherRef) + WireFormatting.StringSize(stream);

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            offset += WireFormatting.WriteString(span[offset..], publisherRef);
            offset += WireFormatting.WriteString(span[offset..], stream);
            writer.Advance(offset);
            return offset;
        }
    }
}
