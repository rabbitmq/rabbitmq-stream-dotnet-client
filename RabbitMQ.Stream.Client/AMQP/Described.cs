using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace RabbitMQ.Stream.Client.AMQP
{
    public readonly struct Described
    {
        public const int DecoderSize = 3;

        public static byte ExtractCode(ReadOnlySequence<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var descriptor);
            WireFormatting.ReadByte(amqpData.Slice(offset), out var value);
            return value;
        }
        
        public static int WriteDescriptor(Span<byte> span, byte data)
        {
            var offset = WireFormatting.WriteByte(span, 0x00);
            offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.SmallUlong);
            offset += WireFormatting.WriteByte(span.Slice(offset), data);
            return offset;
        }
    }
}