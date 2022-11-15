// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;

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

        public ReadOnlySequence<byte> Contents => data;

        public int Size => AmqpWireFormatting.GetSequenceSize(data) + DescribedFormatCode.Size;

        public int Write(Span<byte> span)
        {
            var offset = DescribedFormatCode.Write(span, DescribedFormatCode.ApplicationData);
            if (data.Length <= byte.MaxValue)
            {
                offset += WireFormatting.WriteByte(span[offset..], FormatCode.Vbin8); //binary marker
                offset += WireFormatting.WriteByte(span[offset..], (byte)data.Length); //length
            }
            else
            {
                offset += WireFormatting.WriteByte(span[offset..], FormatCode.Vbin32); //binary marker
                offset += WireFormatting.WriteUInt32(span[offset..], (uint)data.Length); //length
            }

            offset += WireFormatting.Write(span[offset..], data);
            return offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Data Parse(ref SequenceReader<byte> reader, ref int byteRead)
        {
            var offset = AmqpWireFormatting.ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Vbin8:
                    offset += WireFormatting.ReadByte(ref reader, out var len8);
                    byteRead += offset + len8;
                    var data = reader.Sequence.Slice(reader.Position, len8);
                    reader.Advance(len8);
                    return new Data(data);
                case FormatCode.Vbin32:
                    offset += WireFormatting.ReadUInt32(ref reader, out var len32);
                    byteRead += (int)(offset + len32);
                    var data32 = reader.Sequence.Slice(reader.Position, len32);
                    reader.Advance(len32);
                    return new Data(data32);
            }

            throw new AmqpParseException($"Can't parse data is type {type} not defined");
        }
    }
}
