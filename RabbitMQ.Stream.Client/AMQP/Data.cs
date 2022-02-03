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

        public int Size => AmqpWireFormatting.GetSequenceSize(this.data) + AmqpWireFormatting.DecoderSize;

        public int Write(Span<byte> span)
        {
            var offset = new Described(0,
                FormatCode.SmallUlong,
                Codec.ApplicationData).Write(span);
            offset += AmqpWireFormatting.WriteData(span.Slice(offset), data);
            return offset;
        }

        public static Data Parse(ReadOnlySequence<byte> amqpData, out int offset)
        {
            offset = AmqpWireFormatting.ReadBytes(amqpData, out var readOnlySequence);
            return new Data(readOnlySequence);
        }
    }
}