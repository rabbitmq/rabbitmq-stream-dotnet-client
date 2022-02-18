// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CreditRequest : ICommand
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

        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteByte(span.Slice(offset), subscriptionId);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), credit);
            return offset;
        }
    }
}
