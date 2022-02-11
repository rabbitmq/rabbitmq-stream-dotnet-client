using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static class DescribedFormatCode
    {
        public const int Size = 3;

        public static byte Read(ReadOnlySequence<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var descriptor);
            WireFormatting.ReadByte(amqpData.Slice(offset), out var value);
            return value;
        }

        public static int Write(Span<byte> span, byte data)
        {
            var offset = WireFormatting.WriteByte(span, FormatCode.Described);
            offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.SmallUlong);
            offset += WireFormatting.WriteByte(span.Slice(offset), data);
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