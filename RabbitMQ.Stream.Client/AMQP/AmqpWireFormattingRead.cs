// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Text;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static partial class AmqpWireFormatting
    {
        private static readonly Encoding s_encoding = Encoding.UTF8;

        internal static int ReadType(ReadOnlySequence<byte> seq, out byte value)
        {
            var read = WireFormatting.ReadByte(seq, out value);
            return read;
        }

        internal static int ReadBool(ReadOnlySequence<byte> seq, out bool value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Bool:
                    offset = WireFormatting.ReadByte(seq.Slice(offset), out var valueB);
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

        internal static int ReadUshort(ReadOnlySequence<byte> seq, out ushort value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Ushort:
                    offset += WireFormatting.ReadUInt16(seq.Slice(offset), out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUshort invalid type {type}");
        }

        internal static int ReadAny(ReadOnlySequence<byte> seq, out object value)
        {
            // Just pick the type. Won't move the offset since it will be read 
            // again inside each read.
            ReadType(seq, out var type);
            int offset;
            switch (type)
            {
                case FormatCode.Sym32:
                case FormatCode.Sym8:
                case FormatCode.Str8:
                case FormatCode.Str32:

                    offset = ReadString(seq, out var resultString);
                    value = resultString;
                    return offset;
                case FormatCode.Ubyte:
                    offset = ReadUByte(seq, out var resultByte);
                    value = resultByte;
                    return offset;

                case FormatCode.Bool:
                case FormatCode.BoolTrue:
                case FormatCode.BoolFalse:
                    offset = ReadBool(seq, out var resultBool);
                    value = resultBool;
                    return offset;
                case FormatCode.Ushort:
                    offset = ReadUshort(seq, out var resultUshort);
                    value = resultUshort;
                    return offset;
                case FormatCode.Uint:
                case FormatCode.SmallUint:
                case FormatCode.Uint0:
                    offset = ReadUint32(seq, out var resultUint);
                    value = resultUint;
                    return offset;
                case FormatCode.Int:
                case FormatCode.Smallint:
                    offset = ReadInt32(seq, out var resultInt);
                    value = resultInt;
                    return offset;

                case FormatCode.Ulong0:
                case FormatCode.SmallUlong:
                case FormatCode.Ulong:
                    offset = ReadUInt64(seq, out var resultULong);
                    value = resultULong;
                    return offset;
                case FormatCode.Smalllong:
                case FormatCode.Long:
                    offset = ReadInt64(seq, out var resultLong);
                    value = resultLong;
                    return offset;

                case FormatCode.Byte:
                    offset = ReadSByte(seq, out var resultSbyte);
                    value = resultSbyte;
                    return offset;

                case FormatCode.Short:
                    offset = ReadInt16(seq, out var resultShort);
                    value = resultShort;
                    return offset;
                case FormatCode.Timestamp:
                    offset = ReadTimestamp(seq, out var resultTimeStamp);
                    value = resultTimeStamp;
                    return offset;
                case FormatCode.Float:
                    offset = ReadFloat(seq, out var resultFloat);
                    value = resultFloat;
                    return offset;
                case FormatCode.Double:
                    offset = ReadDouble(seq, out var resultDouble);
                    value = resultDouble;
                    return offset;

                case FormatCode.Vbin8:
                case FormatCode.Vbin32:
                    offset = ReadBinary(seq, out var resultBin);
                    value = resultBin;
                    return offset;
                case FormatCode.Null:
                    value = null;
                    return 1;
            }

            throw new AMQP.AmqpParseException($"Read Any: Invalid type: {type}");
        }

        internal static int ReadTimestamp(ReadOnlySequence<byte> seq, out DateTime value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Timestamp:
                    offset += WireFormatting.ReadInt64(seq.Slice(offset), out var ms);
                    var dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(ms);
                    value = dateTimeOffset.DateTime;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadTimestamp invalid type {type}");
        }

        internal static int ReadString(ReadOnlySequence<byte> seq, out string value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Str8:
                case FormatCode.Sym8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenC);
                    value = Encoding.UTF8.GetString(seq.Slice(offset, lenC));
                    return offset + s_encoding.GetBytes(value).Length;

                case FormatCode.Sym32:
                case FormatCode.Str32:
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out var len);
                    value = Encoding.UTF8.GetString(seq.Slice(offset, len));
                    return offset + s_encoding.GetBytes(value).Length;
            }

            throw new AMQP.AmqpParseException($"ReadString invalid type {type}");
        }

        internal static int ReadUInt64(ReadOnlySequence<byte> seq, out ulong value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Ulong0:
                    value = 0;
                    return offset;
                case FormatCode.SmallUlong:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenB);
                    value = lenB;
                    return offset;
                case FormatCode.Ulong:
                    offset += WireFormatting.ReadUInt64(seq.Slice(offset), out var lenI);
                    value = lenI;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUlong Invalid type {type}");
        }

        internal static int ReadInt64(ReadOnlySequence<byte> seq, out long value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Smalllong:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenB);
                    value = lenB;
                    return offset;
                case FormatCode.Long:
                    offset += WireFormatting.ReadInt64(seq.Slice(offset), out var lenI);
                    value = lenI;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUlong Invalid type {type}");
        }

        internal static int ReadFloat(ReadOnlySequence<byte> seq, out float value)
        {
            var offset = ReadType(seq, out var type);

            switch (type)
            {
                case FormatCode.Float:
                    offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var valueUint);
                    value = BitConverter.ToSingle(BitConverter.GetBytes(valueUint), 0);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadFloat Invalid type {type}");
        }

        internal static int ReadDouble(ReadOnlySequence<byte> seq, out double value)
        {
            var offset = ReadType(seq, out var type);

            switch (type)
            {
                case FormatCode.Double:
                    offset += WireFormatting.ReadInt64(seq.Slice(offset), out var valueInt);
                    value = BitConverter.ToDouble(BitConverter.GetBytes(valueInt), 0);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadDouble Invalid type {type}");
        }

        internal static int ReadSByte(ReadOnlySequence<byte> seq, out sbyte value)
        {
            var offset = ReadType(seq, out var type);

            switch (type)
            {
                case FormatCode.Byte:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var valueSbyte);
                    value = (sbyte)valueSbyte;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadSByte Invalid type {type}");
        }

        internal static int ReadInt16(ReadOnlySequence<byte> seq, out short value)
        {
            var offset = ReadType(seq, out var type);

            switch (type)
            {
                case FormatCode.Short:
                    offset += WireFormatting.ReadInt16(seq.Slice(offset), out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadInt16 Invalid type {type}");
        }

        internal static int ReadBinary(ReadOnlySequence<byte> seq, out byte[] value)
        {
            var offset = ReadType(seq, out var type);
            int length;
            switch (type)
            {
                case FormatCode.Vbin8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenV);
                    length = lenV;
                    break;
                case FormatCode.Vbin32:
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out var lenI);
                    length = lenI;
                    break;
                default:
                    throw new AMQP.AmqpParseException($"ReadBinary Invalid type {type}");
            }

            offset += WireFormatting.ReadBytes(seq.Slice(offset), length, out value);
            return offset;

            //TODO Wire implement the size/len verification
        }

        internal static int ReadMapHeader(ReadOnlySequence<byte> seq, out uint count)
        {
            var offset = ReadType(seq, out var type);

            switch (type)
            {
                case FormatCode.Map8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out _);
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var len8);
                    count = len8;
                    return offset;
                case FormatCode.Map32:
                    offset += WireFormatting.ReadUInt32(seq.Slice(offset), out _);
                    offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var len32);
                    count = len32;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadMapHeader Invalid type {type}");
        }

        internal static int ReadListHeader(ReadOnlySequence<byte> seq, out long lenght)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.List0:
                    lenght = 0;
                    return offset;
                case FormatCode.List8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out _);
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenB);
                    lenght = lenB;
                    // size := int(buf[0])
                    // if size > listLength-1 {
                    //     return 0, errorNew("invalid length")
                    // }
                    return offset;
                case FormatCode.List32:
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out _);
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out var lenI);
                    lenght = lenI;
                    // size := int(buf[0])
                    // if size > listLength-1 {
                    //     return 0, errorNew("invalid length")
                    // }
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadCompositeHeader Invalid type {type}");
        }

        internal static int ReadCompositeHeader(ReadOnlySequence<byte> seq, out long fields, out byte next)
        {
            var offset = ReadType(seq, out var type);

            // compsites always start with 0x0
            if (type != 0)
            {
                // TODO WIRE
                throw new AmqpParseException($"invalid composite header %#02x {type}");
            }

            // next, the composite type is encoded as an AMQP uint8
            offset += ReadUInt64(seq.Slice(offset), out var nextW);
            next = (byte)nextW;
            // fields are represented as a list
            offset += ReadListHeader(seq.Slice(offset), out fields);
            return offset;
        }

        internal static int ReadUint32(ReadOnlySequence<byte> seq, out uint value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Uint0:
                    value = 0;
                    return offset;
                case FormatCode.SmallUint:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var valueB);
                    value = valueB;
                    return offset;
                case FormatCode.Uint:
                    offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var valueUi);
                    value = valueUi;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUint32 Invalid type {type}");
        }

        internal static int ReadInt32(ReadOnlySequence<byte> seq, out int value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Smallint:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var valueB);
                    value = valueB;
                    return offset;
                case FormatCode.Int:
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadInt32 Invalid type {type}");
        }

        internal static int ReadUByte(ReadOnlySequence<byte> seq, out byte value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Ubyte:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUbyte Invalid type {type}");
        }

        internal static int TryReadNull(ReadOnlySequence<byte> seq, out bool value)
        {
            ReadType(seq, out var type);
            if (seq.Length > 0 && type == FormatCode.Null)
            {
                value = true;
                return 1;
            }

            value = false;
            return 0;
        }
    }
}
