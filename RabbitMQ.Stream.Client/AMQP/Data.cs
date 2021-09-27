using System;
using System.Buffers;
using System.Threading;

namespace RabbitMQ.Stream.Client.AMQP
{
    public interface IWritable
    {
        public int Size { get; }
        public int Write(Span<byte> span);
    }
    
    public readonly struct Data : IWritable
    {
        private readonly ReadOnlySequence<byte> data;

        public Data(ReadOnlySequence<byte> data)
        {
            this.data = data;
        }

        public ReadOnlySequence<byte> Contents => this.data;

        public int Size
        {
            get
            {
                if (data.Length < 256)
                    return (int)data.Length + 5;
                return (int) data.Length + 8;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteByte(span, 0); //descriptor marker
            offset += WireFormatting.WriteByte(span.Slice(offset), 0x53); //short ulong
            offset += WireFormatting.WriteByte(span.Slice(offset), 117); //data code number
            if (data.Length < 256)
            {
                offset += WireFormatting.WriteByte(span.Slice(offset), 0xA0); //binary marker
                offset += WireFormatting.WriteByte(span.Slice(offset), (byte)data.Length); //length
            }
            else
            {
                offset += WireFormatting.WriteByte(span.Slice(offset), 0xB0); //binary marker
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)data.Length); //length
            }

            offset += WireFormatting.Write(span.Slice(offset), data);
            return offset;
        }

        public static Data Parse(ReadOnlySequence<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var descriptor);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var dataCode);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var binaryMarker);
            switch (binaryMarker)
            {
                case 0xA0:
                {
                    offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var length);
                    return new Data(amqpData.Slice(offset, length));
                }
                case 0xB0:
                {
                    offset += WireFormatting.ReadUInt32(amqpData.Slice(offset), out var length);
                    return new Data(amqpData.Slice(offset, (int)length));
                }
            }

            throw new AmqpParseException("failed to parse data");
        }
    }
}