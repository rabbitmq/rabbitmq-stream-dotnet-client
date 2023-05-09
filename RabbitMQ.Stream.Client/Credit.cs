// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct CreditRequest : ICommand
    {
        private readonly byte subscriptionId;
        private readonly ushort credit;
        public const ushort Key = 9;

        public CreditRequest(byte subscriptionId, ushort credit)
        {
            this.subscriptionId = subscriptionId;
            this.credit = credit;
        }
        public int SizeNeeded => 7;

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
            offset += WireFormatting.WriteByte(span[offset..], subscriptionId);
            offset += WireFormatting.WriteUInt16(span[offset..], credit);
            writer.Advance(offset);
            return offset;
        }
    }
}
