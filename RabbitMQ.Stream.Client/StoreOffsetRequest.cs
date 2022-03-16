﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct StoreOffsetRequest : ICommand
    {
        public const ushort Key = 10;
        private readonly string stream;
        private readonly string reference;
        private readonly ulong offsetValue;

        public StoreOffsetRequest(string stream, string reference, ulong offsetValue)
        {
            this.stream = stream;
            this.reference = reference;
            this.offsetValue = offsetValue;
        }

        public int SizeNeeded =>
            2 + 2 + WireFormatting.StringSize(reference) + WireFormatting.StringSize(stream) + 8;

        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteString(span.Slice(offset), reference);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            offset += WireFormatting.WriteUInt64(span.Slice(offset), offsetValue);
            return offset;
        }
    }
}
