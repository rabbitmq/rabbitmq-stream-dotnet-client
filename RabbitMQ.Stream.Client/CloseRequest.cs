// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct CloseRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string reason;
        public const ushort Key = 22;

        public CloseRequest(uint correlationId, string reason)
        {
            this.correlationId = correlationId;
            this.reason = reason;
        }

        public int SizeNeeded => 10 + WireFormatting.StringSize(reason);

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            offset += WireFormatting.WriteUInt16(span[offset..], 1); //ok code
            offset += WireFormatting.WriteString(span[offset..], reason);
            writer.Advance(offset);
            return offset;
        }
    }
}
