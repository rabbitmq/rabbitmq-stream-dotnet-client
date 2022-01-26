using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public readonly struct AmqpFrameHeader : IWritable
    {
        public AmqpFrameHeader(byte marker, byte descriptor, byte dataCode)
        {
            this.Marker = marker;
            this.Descriptor =  descriptor;
            this.DataCode =  dataCode;
        }

        public byte Marker { get; }
        public byte Descriptor { get; }
        public byte DataCode { get; }

        public int Size => 1 + 1 + 1;

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteByte(span, Marker);
            offset += WireFormatting.WriteByte(span.Slice(offset),  Descriptor);
            offset += WireFormatting.WriteByte(span.Slice(offset),  DataCode);
            return offset;
        }

        public static AmqpFrameHeader Parse(ReadOnlySequence<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var descriptor);
            WireFormatting.ReadByte(amqpData.Slice(offset), out var dataCode);
            return new AmqpFrameHeader(marker, descriptor, dataCode);
        }
    }


    public readonly struct Annotations
    {
        private Annotations(Dictionary<object, object> items, int size)
        {
            this.Items = items;
            Size = size;
        }

        public Dictionary<object, object> Items { get; }
        public int Size { get; }

        public static Annotations Parse(ReadOnlySequence<byte> amqpData)
        {
            var offset = AmqpWireFormatting.ReadMapHeader(amqpData, out var count);
            var annotations = new Dictionary<object, object>();
            var values = (count / 2);
            for (int i = 0; i < values; i++)
            {
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var key);
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var value);
                annotations[key] = value;
            }

            return new Annotations(annotations, offset);
        }
    }
}