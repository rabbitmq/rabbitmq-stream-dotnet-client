using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public readonly struct Described : IWritable
    {
        public Described(byte marker, byte descriptor, byte dataCode)
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

        public static Described Parse(ReadOnlySequence<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var descriptor);
            WireFormatting.ReadByte(amqpData.Slice(offset), out var dataCode);
            return new Described(marker, descriptor, dataCode);
        }
    }


    
}