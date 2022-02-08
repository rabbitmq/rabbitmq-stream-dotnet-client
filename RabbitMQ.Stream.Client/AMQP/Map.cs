using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public abstract class Map<TKey> : Dictionary<TKey, object>, IWritable where TKey : class
    {
        protected byte MapCode;
        
        public static T Parse<T>(ReadOnlySequence<byte> amqpData, ref int byteRead) where T : Map<TKey>, new()
        {
            var offset = AmqpWireFormatting.ReadMapHeader(amqpData, out var count);
            var amqpMap = new T();
            var values = (count / 2);
            for (var i = 0; i < values; i++)
            {
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var key);
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var value);
                amqpMap[(key as TKey)!] = value;
            }

            byteRead += offset;
            return amqpMap;
        }


        private int MapSize()
        {
            var size = 0;
            foreach (var (key, value) in this)
            {
                size += AmqpWireFormatting.GetAnySize(key);
                size += AmqpWireFormatting.GetAnySize(value);
            }

            return size;
        }

        public int Size
        {
            get
            {
                var size = Described.DecoderSize;
                size += 1; //FormatCode.List32
                size += 4; // field numbers
                size += 4; // PropertySize
                size += MapSize(); // PropertySize
                return size;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = Described.WriteDescriptor(span, MapCode);
            offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.Map32);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) MapSize()); // MapSize 
            offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) this.Count * 2); // pair values
            foreach (var (key, value) in this)
            {
                offset += AmqpWireFormatting.WriteAny(span.Slice(offset), key);
                offset += AmqpWireFormatting.WriteAny(span.Slice(offset), value);
            }
            return offset;
        }
    }
}