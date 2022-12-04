// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public abstract class Map<TKey> : Dictionary<TKey, object>, IWritable where TKey : class
    {
        protected byte MapDataCode;

        public static T Parse<T>(ReadOnlySequence<byte> amqpData, ref int byteRead) where T : Map<TKey>, new()
        {
            var offset = AmqpWireFormatting.ReadMapHeader(amqpData, out var count);
            var amqpMap = new T();
            var values = (count / 2);
            for (var i = 0; i < values; i++)
            {
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var key);
                offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var value);
                if (!IsNullOrEmptyString(key)) // this should never occur because we never write null keys
                {
                    amqpMap[(key as TKey)!] = value;
                }
            }

            byteRead += offset;
            return amqpMap;
        }

        private int MapSize()
        {
            var size = 0;
            foreach (var (key, value) in this)
            {
                if (!IsNullOrEmptyString(key))
                {
                    size += AmqpWireFormatting.GetAnySize(key);
                    size += AmqpWireFormatting.GetAnySize(value);
                }
            }

            return size;
        }

        private static bool IsNullOrEmptyString(object value)
        {
            return value switch
            {
                null => true,
                string s => string.IsNullOrEmpty(s),
                _ => false
            };
        }

        public int Size
        {
            get
            {
                var size = DescribedFormatCode.Size;
                size += sizeof(byte); //FormatCode.List32
                size += sizeof(uint); // field numbers
                size += sizeof(uint); // PropertySize
                size += MapSize();
                return size;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = DescribedFormatCode.Write(span, MapDataCode);
            offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.Map32);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)MapSize()); // MapSize 
            offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)Count * 2); // pair values
            foreach (var (key, value) in this)
            {
                if (!IsNullOrEmptyString(key))
                {
                    offset += AmqpWireFormatting.WriteAny(span.Slice(offset), key);
                    offset += AmqpWireFormatting.WriteAny(span.Slice(offset), value);
                }
            }

            return offset;
        }
    }
}
