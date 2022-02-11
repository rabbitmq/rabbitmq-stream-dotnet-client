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

        public int Size => AmqpWireFormatting.GetSequenceSize(this.data) + DescribedFormatCode.Size;

        public int Write(Span<byte> span)
        {
            var offset = DescribedFormatCode.Write(span, DescribedFormatCode.ApplicationData);
            if (data.Length < byte.MaxValue)
            {
                offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.Vbin8); //binary marker
                offset += WireFormatting.WriteByte(span.Slice(offset), (byte) data.Length); //length
            }
            else
            {
                offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.Vbin32); //binary marker
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) data.Length); //length
            }

            offset += WireFormatting.Write(span.Slice(offset), data);
            return offset;
        }


        public static Data Parse(ReadOnlySequence<byte> amqpData, ref int byteRead)
        {
            var offset = AmqpWireFormatting.ReadType(amqpData, out var type);
            switch (type)
            {
                case FormatCode.Vbin8:
                    offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var len8);
                    byteRead += offset + len8;
                    return new Data(amqpData.Slice(offset, len8));
                case FormatCode.Vbin32:
                    offset += WireFormatting.ReadUInt32(amqpData.Slice(offset), out var len32);
                    byteRead += (int) (offset + len32);
                    return new Data(amqpData.Slice(offset, len32));
            }

            throw new AMQP.AmqpParseException($"Can't parse data is type {type} not defined");
        }
    }
}