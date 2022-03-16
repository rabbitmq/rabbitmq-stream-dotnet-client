// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct OpenRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string vhost;
        public const ushort Key = 21;

        public OpenRequest(uint correlationId, string vhost)
        {
            this.correlationId = correlationId;
            this.vhost = vhost;
        }
        public int SizeNeeded => 8 + WireFormatting.StringSize(vhost);

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), vhost);
            return offset;
        }
    }
}
