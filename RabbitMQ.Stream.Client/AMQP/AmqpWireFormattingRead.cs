// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static partial class AmqpWireFormatting
    {
        private static readonly Encoding s_encoding = Encoding.UTF8;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        internal static void PeekType(ref SequenceReader<byte> reader, out byte value)
        {
            reader.TryPeek(out value);
        }
        internal static int ReadType(ref SequenceReader<byte> reader, out byte value)
        {
            var read = WireFormatting.ReadByte(ref reader, out value);
            return read;
        }

        internal static int ReadBool(ref SequenceReader<byte> reader, out bool value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Bool:
                    offset = WireFormatting.ReadByte(ref reader, out var valueB);
                    value = valueB != 0;
                    return offset;
                case FormatCode.BoolTrue:
                    value = true;
                    return offset;
                case FormatCode.BoolFalse:
                    value = false;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadBool invalid type {type}");
        }

        internal static int ReadUshort(ref SequenceReader<byte> reader, out ushort value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Ushort:

                    offset += WireFormatting.ReadUInt16(ref reader, out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUshort invalid type {type}");
        }

        internal static int ReadAny(ref SequenceReader<byte> reader, out object value)
        {
            // Just pick the type. Won't move the offset since it will be read 
            // again inside each read.
            PeekType(ref reader, out var type);
            int offset;
            switch (type)
            {
                case FormatCode.Sym32:
                case FormatCode.Sym8:
                case FormatCode.Str8:
                case FormatCode.Str32:

                    offset = ReadString(ref reader, out var resultString);
                    value = resultString;
                    return offset;
                case FormatCode.Ubyte:
                    offset = ReadUByte(ref reader, out var resultByte);
                    value = resultByte;
                    return offset;

                case FormatCode.Bool:
                case FormatCode.BoolTrue:
                case FormatCode.BoolFalse:
                    offset = ReadBool(ref reader, out var resultBool);
                    value = resultBool;
                    return offset;
                case FormatCode.Ushort:
                    offset = ReadUshort(ref reader, out var resultUshort);
                    value = resultUshort;
                    return offset;
                case FormatCode.Uint:
                case FormatCode.SmallUint:
                case FormatCode.Uint0:
                    offset = ReadUint32(ref reader, out var resultUint);
                    value = resultUint;
                    return offset;
                case FormatCode.Int:
                case FormatCode.Smallint:
                    offset = ReadInt32(ref reader, out var resultInt);
                    value = resultInt;
                    return offset;

                case FormatCode.Ulong0:
                case FormatCode.SmallUlong:
                case FormatCode.Ulong:
                    offset = ReadUInt64(ref reader, out var resultULong);
                    value = resultULong;
                    return offset;
                case FormatCode.Smalllong:
                case FormatCode.Long:
                    offset = ReadInt64(ref reader, out var resultLong);
                    value = resultLong;
                    return offset;

                case FormatCode.Byte:
                    offset = ReadSByte(ref reader, out var resultSbyte);
                    value = resultSbyte;
                    return offset;

                case FormatCode.Short:
                    offset = ReadInt16(ref reader, out var resultShort);
                    value = resultShort;
                    return offset;
                case FormatCode.Timestamp:
                    offset = ReadTimestamp(ref reader, out var resultTimeStamp);
                    value = resultTimeStamp;
                    return offset;
                case FormatCode.Float:
                    offset = ReadFloat(ref reader, out var resultFloat);
                    value = resultFloat;
                    return offset;
                case FormatCode.Double:
                    offset = ReadDouble(ref reader, out var resultDouble);
                    value = resultDouble;
                    return offset;

                case FormatCode.Vbin8:
                case FormatCode.Vbin32:
                    offset = ReadBinary(ref reader, out var resultBin);
                    value = resultBin;
                    return offset;
                case FormatCode.Null:
                    value = null;
                    reader.Advance(1);
                    return 1;
            }

            throw new AMQP.AmqpParseException($"Read Any: Invalid type: {type}");
        }

        internal static int ReadTimestamp(ref SequenceReader<byte> reader, out DateTime value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Timestamp:
                    offset += WireFormatting.ReadInt64(ref reader, out var ms);
                    var dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(ms);
                    value = dateTimeOffset.DateTime;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadTimestamp invalid type {type}");
        }

        internal static int ReadString(ref SequenceReader<byte> reader, out string value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Str8:
                case FormatCode.Sym8:
                    offset += WireFormatting.ReadByte(ref reader, out var lenC);
                    Span<byte> tempSpan = stackalloc byte[lenC];
                    reader.TryCopyTo(tempSpan);
                    reader.Advance(lenC);
                    value = Encoding.UTF8.GetString(tempSpan);
                    return offset + s_encoding.GetByteCount(value);

                case FormatCode.Sym32:
                case FormatCode.Str32:
                    offset += WireFormatting.ReadInt32(ref reader, out var len);
                    var tempSpan32 = len <= 64 ? stackalloc byte[len] : new byte[len];
                    reader.TryCopyTo(tempSpan32);
                    reader.Advance(len);
                    value = Encoding.UTF8.GetString(tempSpan32);
                    return offset + s_encoding.GetByteCount(value);
            }

            throw new AMQP.AmqpParseException($"ReadString invalid type {type}");
        }

        internal static int ReadUInt64(ref SequenceReader<byte> reader, out ulong value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Ulong0:
                    value = 0;
                    return offset;
                case FormatCode.SmallUlong:
                    offset += WireFormatting.ReadByte(ref reader, out var lenB);
                    value = lenB;
                    return offset;
                case FormatCode.Ulong:
                    offset += WireFormatting.ReadUInt64(ref reader, out var lenI);
                    value = lenI;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUlong Invalid type {type}");
        }

        internal static int ReadInt64(ref SequenceReader<byte> reader, out long value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Smalllong:
                    offset += WireFormatting.ReadByte(ref reader, out var lenB);
                    value = lenB;
                    return offset;
                case FormatCode.Long:
                    offset += WireFormatting.ReadInt64(ref reader, out var lenI);
                    value = lenI;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUlong Invalid type {type}");
        }

        internal static int ReadFloat(ref SequenceReader<byte> reader, out float value)
        {
            var offset = ReadType(ref reader, out var type);

            switch (type)
            {
                case FormatCode.Float:
                    offset += WireFormatting.ReadUInt32(ref reader, out var valueUint);
                    value = BitConverter.ToSingle(BitConverter.GetBytes(valueUint), 0);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadFloat Invalid type {type}");
        }

        internal static int ReadDouble(ref SequenceReader<byte> reader, out double value)
        {
            var offset = ReadType(ref reader, out var type);

            switch (type)
            {
                case FormatCode.Double:
                    offset += WireFormatting.ReadInt64(ref reader, out var valueInt);
                    value = BitConverter.ToDouble(BitConverter.GetBytes(valueInt), 0);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadDouble Invalid type {type}");
        }

        internal static int ReadSByte(ref SequenceReader<byte> reader, out sbyte value)
        {
            var offset = ReadType(ref reader, out var type);

            switch (type)
            {
                case FormatCode.Byte:
                    offset += WireFormatting.ReadByte(ref reader, out var valueSbyte);
                    value = (sbyte)valueSbyte;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadSByte Invalid type {type}");
        }

        internal static int ReadInt16(ref SequenceReader<byte> reader, out short value)
        {
            var offset = ReadType(ref reader, out var type);

            switch (type)
            {
                case FormatCode.Short:
                    offset += WireFormatting.ReadInt16(ref reader, out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadInt16 Invalid type {type}");
        }

        internal static int ReadBinary(ref SequenceReader<byte> reader, out byte[] value)
        {
            var offset = ReadType(ref reader, out var type);
            int length;
            switch (type)
            {
                case FormatCode.Vbin8:
                    offset += WireFormatting.ReadByte(ref reader, out var lenV);
                    length = lenV;
                    break;
                case FormatCode.Vbin32:
                    offset += WireFormatting.ReadInt32(ref reader, out var lenI);
                    length = lenI;
                    break;
                default:
                    throw new AMQP.AmqpParseException($"ReadBinary Invalid type {type}");
            }

            offset += WireFormatting.ReadBytes(ref reader, length, out value);
            return offset;

            //TODO Wire implement the size/len verification
        }

        internal static int ReadMapHeader(ref SequenceReader<byte> reader, out uint count)
        {
            var offset = ReadType(ref reader, out var type);

            switch (type)
            {
                case FormatCode.Map8:
                    offset += WireFormatting.ReadByte(ref reader, out _);
                    offset += WireFormatting.ReadByte(ref reader, out var len8);
                    count = len8;
                    return offset;
                case FormatCode.Map32:
                    offset += WireFormatting.ReadUInt32(ref reader, out _);
                    offset += WireFormatting.ReadUInt32(ref reader, out var len32);
                    count = len32;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadMapHeader Invalid type {type}");
        }

        internal static int ReadListHeader(ref SequenceReader<byte> reader, out long lenght)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.List0:
                    lenght = 0;
                    return offset;
                case FormatCode.List8:
                    offset += WireFormatting.ReadByte(ref reader, out _);
                    offset += WireFormatting.ReadByte(ref reader, out var lenB);
                    lenght = lenB;
                    // size := int(buf[0])
                    // if size > listLength-1 {
                    //     return 0, errorNew("invalid length")
                    // }
                    return offset;
                case FormatCode.List32:
                    offset += WireFormatting.ReadInt32(ref reader, out _);
                    offset += WireFormatting.ReadInt32(ref reader, out var lenI);
                    lenght = lenI;
                    // size := int(buf[0])
                    // if size > listLength-1 {
                    //     return 0, errorNew("invalid length")
                    // }
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadCompositeHeader Invalid type {type}");
        }

        internal static int ReadCompositeHeader(ref SequenceReader<byte> reader, out long fields, out byte next)
        {
            var offset = ReadType(ref reader, out var type);

            // composite always start with 0x0
            if (type != 0)
            {
                // TODO WIRE
                throw new AmqpParseException($"invalid composite header %#02x {type}");
            }

            // next, the composite type is encoded as an AMQP uint8
            offset += ReadUInt64(ref reader, out var nextW);
            next = (byte)nextW;
            // fields are represented as a list
            offset += ReadListHeader(ref reader, out fields);
            return offset;
        }

        internal static int ReadUint32(ref SequenceReader<byte> reader, out uint value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Uint0:
                    value = 0;
                    return offset;
                case FormatCode.SmallUint:
                    offset += WireFormatting.ReadByte(ref reader, out var valueB);
                    value = valueB;
                    return offset;
                case FormatCode.Uint:
                    offset += WireFormatting.ReadUInt32(ref reader, out var valueUi);
                    value = valueUi;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUint32 Invalid type {type}");
        }

        internal static int ReadInt32(ref SequenceReader<byte> reader, out int value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Smallint:
                    offset += WireFormatting.ReadByte(ref reader, out var valueB);
                    value = valueB;
                    return offset;
                case FormatCode.Int:
                    offset += WireFormatting.ReadInt32(ref reader, out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadInt32 Invalid type {type}");
        }

        internal static int ReadUByte(ref SequenceReader<byte> reader, out byte value)
        {
            var offset = ReadType(ref reader, out var type);
            switch (type)
            {
                case FormatCode.Ubyte:
                    offset += WireFormatting.ReadByte(ref reader, out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUbyte Invalid type {type}");
        }

        internal static int TryReadNull(ref SequenceReader<byte> reader, out bool value)
        {
            PeekType(ref reader, out var type);
            if (reader.Remaining > 0 && type == FormatCode.Null)
            {
                value = true;
                reader.Advance(1);
                return 1;
            }

            value = false;
            return 0;
        }
    }
}
