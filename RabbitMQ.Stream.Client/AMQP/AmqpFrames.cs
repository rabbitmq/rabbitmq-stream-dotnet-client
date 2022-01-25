using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public abstract class AbstractAmqpFrame
    {
        protected virtual void Parse(ReadOnlySequence<byte> amqpData)
        {
        }
    }

    public readonly struct MessageType
    {
        public MessageType(byte marker, byte descriptor, byte dataCode)
        {
            this.Marker = marker;
            this.Descriptor = descriptor;
            this.DataCode = (FrameType) dataCode;
        }

        public byte Marker { get; }
        public byte Descriptor { get; }
        public FrameType DataCode { get; }

        public int Size => 1 + 1 + 1;

        public static MessageType Parse(ReadOnlySequence<byte> amqpData)
        {
            var offset = WireFormatting.ReadByte(amqpData, out var marker);
            offset += WireFormatting.ReadByte(amqpData.Slice(offset), out var descriptor);
            WireFormatting.ReadByte(amqpData.Slice(offset), out var dataCode);
            return new MessageType(marker, descriptor, dataCode);
        }
    }


    public readonly struct Annotations
    {
        public Annotations(Dictionary<object, object> items, int size)
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