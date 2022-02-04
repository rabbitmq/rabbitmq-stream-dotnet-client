using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static partial class AmqpWireFormatting
    {
        private static Encoding s_encoding = Encoding.UTF8;

        internal static int ReadType(ReadOnlySequence<byte> seq, out byte value)
        {
            var read = WireFormatting.ReadByte(seq, out value);
            return read;
        }


        private static int ReadBool(ReadOnlySequence<byte> seq, out bool value)
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
            var offset = 0;
            // Just pick the type. Won't move the offset since it will be read 
            // again inside each read.
            ReadType(seq, out var type);
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
            }

            throw new AMQP.AmqpParseException($"can't read any {type}");
        }


        internal static int ReadTimestamp(ReadOnlySequence<byte> seq, out DateTime value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Timestamp:
                    offset += WireFormatting.ReadInt64(seq.Slice(offset), out var ms);
                    DateTimeOffset dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(ms);
                    value = dateTimeOffset.DateTime;
                    return offset;
                //TODO wire check date time
                // return time.Unix(ms/1000, (ms%1000)*1000000).UTC(), err
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
                    return offset + lenC;

                case FormatCode.Sym32:
                case FormatCode.Str32:
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out var len);
                    value = Encoding.UTF8.GetString(seq.Slice(offset, len));
                    return offset + len;
            }

            throw new AMQP.AmqpParseException($"ReadString invalid type {type}");
        }

        internal static int ReadUlong(ReadOnlySequence<byte> seq, out long value)
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
                    offset += WireFormatting.ReadInt64(seq.Slice(offset), out var lenI);
                    value = lenI;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUlong Invalid type {type}");
        }


        internal static int ReadBinary(ReadOnlySequence<byte> seq, out byte[] value)
        {
            var offset = ReadType(seq, out var type);
            var length = 0;
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
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var first);
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var len);
                    count = len;
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
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var sizeB);
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenB);
                    lenght = lenB;
                    // size := int(buf[0])
                    // if size > listLength-1 {
                    //     return 0, errorNew("invalid length")
                    // }
                    return offset;
                case FormatCode.List32:
                    offset += WireFormatting.ReadInt32(seq.Slice(offset), out var sizeI);
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
                throw new Exception($"invalid composite header %#02x {type}");
            }

            // next, the composite type is encoded as an AMQP uint8
            offset += ReadUlong(seq.Slice(offset), out var nextW);
            next = (byte) nextW;
            // fields are represented as a list
            offset += ReadListHeader(seq.Slice(offset), out fields);
            return offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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