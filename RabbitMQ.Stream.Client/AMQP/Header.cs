// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client.AMQP
{
    public class Header
    {
        public bool Durable { get; internal set; }
        public byte Priority { get; internal set; }
        public uint Ttl { get; internal set; } // from milliseconds
        public bool FirstAcquirer { get; internal set; }
        public uint DeliveryCount { get; internal set; }

        public static Header Parse(ref SequenceReader<byte> reader, ref int byteRead)
        {
            var offset = AmqpWireFormatting.ReadCompositeHeader(ref reader, out var fields, out _);
            //TODO WIRE check the next
            var h = new Header();
            for (var index = 0; index < fields; index++)
            {
                offset += AmqpWireFormatting.TryReadNull(ref reader, out var value);

                if (!value)
                {
                    switch (index)
                    {
                        case 0:
                            offset += AmqpWireFormatting.ReadAny(ref reader, out var durable);
                            h.Durable = (bool)durable;
                            break;
                        case 1:
                            offset += AmqpWireFormatting.ReadUByte(ref reader, out var priority);
                            h.Priority = priority;
                            break;
                        case 2:
                            offset += AmqpWireFormatting.ReadAny(ref reader, out var ttl);
                            h.Ttl = (uint)ttl;
                            break;
                        case 3:
                            offset += AmqpWireFormatting.ReadBool(ref reader, out var firstAcquirer);
                            h.FirstAcquirer = firstAcquirer;
                            break;
                        case 4:
                            offset += AmqpWireFormatting.ReadUint32(ref reader, out var deliveryCount);
                            h.DeliveryCount = deliveryCount;
                            break;
                    }
                }
            }

            byteRead += offset;
            return h;
        }
    }
}
