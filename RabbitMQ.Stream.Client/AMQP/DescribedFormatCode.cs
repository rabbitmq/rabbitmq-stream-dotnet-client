// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static class DescribedFormatCode
    {
        public const int Size = 3;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte Read(ref SequenceReader<byte> reader)
        {
            // DescribedFormatCode in this case we need to read 
            // only the last byte form the header to get the format code
            // the first can be ignored but it is necessary to read them
            reader.TryRead(out _);
            reader.TryRead(out _);
            reader.TryRead(out var formatCode);
            return formatCode;
        }

        public static int Write(Span<byte> span, byte data)
        {
            var offset = WireFormatting.WriteByte(span, FormatCode.Described);
            offset += WireFormatting.WriteByte(span[offset..], FormatCode.SmallUlong);
            offset += WireFormatting.WriteByte(span[offset..], data);
            return offset;
        }

        public const byte ApplicationData = 0x75;
        public const byte MessageAnnotations = 0x72;
        public const byte MessageProperties = 0x73;
        public const byte ApplicationProperties = 0x74;
        public const byte MessageHeader = 0x70;
        public const byte AmqpValue = 0x77;
    }
}
