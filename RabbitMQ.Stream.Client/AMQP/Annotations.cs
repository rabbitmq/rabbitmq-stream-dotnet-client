using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public class Annotations : Dictionary<string, object>
    {
        private int CalculateSize()
        {
            var size = AmqpWireFormatting.GetFieldValueByteCount(this);
            return size;
        }

        public int Size => CalculateSize();

        public static Annotations Parse(ReadOnlySequence<byte> amqpData, out int offset)
        {
            offset = AmqpWireFormatting.ReadMapHeader(amqpData, out var count);
            var annotations = new Annotations();
            var values = (count / 2);
            for (var i = 0; i < values; i++)
            {
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var key);
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var value);
                annotations[(string) key] = value;
            }

            return annotations;
        }
    }
}