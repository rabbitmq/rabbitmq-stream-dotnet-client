// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static partial class AmqpWireFormatting
    {
        public static int WriteAny(Span<byte> seq, object value)
        {
            return value switch
            {
                null => WriteNull(seq),
                string s => string.IsNullOrEmpty(s) ? WriteNull(seq) : WriteString(seq, s),
                int i => WriteInt(seq, i),
                uint u => WriteUInt(seq, u),
                ulong ul => WriteUInt64(seq, ul),
                long l => WriteInt64(seq, l),
                ushort ush => WriteUInt16(seq, ush),
                byte b => WriteByte(seq, b),
                sbyte sb => WriteSByte(seq, sb),
                short sb => WriteIn16(seq, sb),
                float ft => WriteFloat(seq, ft),
                double db => WriteDouble(seq, db),
                bool bo => WriteBool(seq, bo),
                byte[] bArr => bArr.Length == 0 ? WriteNull(seq) : WriteBytes(seq, bArr),
                DateTime d => d == DateTime.MinValue ? WriteNull(seq) : WriteTimestamp(seq, d),
                _ => throw new AmqpParseException($"WriteAny Invalid type {value}")
            };
        }

        private static int WriteString(Span<byte> seq, string value)
        {
            var len = value.Length;
            var offset = 0;
            // Str8
            if (len < byte.MaxValue)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Str8);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)len);
                offset += s_encoding.GetBytes(value, seq.Slice(offset));
                return offset;
            }

            // Str32
            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Str32);
            offset += WireFormatting.WriteInt32(seq.Slice(offset), len);
            offset += s_encoding.GetBytes(value, seq.Slice(offset));
            return offset;
        }

        private static int WriteUInt64(Span<byte> seq, ulong value)
        {
            if (value == 0)
            {
                return WireFormatting.WriteByte(seq, FormatCode.Ulong0);
            }

            var offset = 0;
            if (value < 256)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.SmallUlong);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)value);
                return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Ulong);
            offset += WireFormatting.WriteUInt64(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteUInt(Span<byte> seq, uint value)

        {
            var offset = 0;
            switch (value)
            {
                case 0:
                    return WireFormatting.WriteByte(seq, FormatCode.Uint0);
                case < 256:
                    offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.SmallUint);
                    offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)value);
                    return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Uint);
            offset += WireFormatting.WriteUInt32(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteInt(Span<byte> seq, int value)
        {
            var offset = 0;
            if (value is < 128 and >= -128)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Smallint);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)value);

                return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Int);
            offset += WireFormatting.WriteInt32(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteInt64(Span<byte> seq, long value)
        {
            var offset = 0;
            if (value is < 128 and >= -128)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Smalllong);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)value);

                return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Long);
            offset += WireFormatting.WriteInt64(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteNull(Span<byte> seq)
        {
            return WireFormatting.WriteByte(seq, FormatCode.Null);
        }

        private static int WriteBytes(Span<byte> seq, byte[] value)
        {
            var len = value.Length;
            var offset = 0;

            // List8
            if (len < 256)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Vbin8);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)len);
                offset += WireFormatting.Write(seq.Slice(offset), new ReadOnlySequence<byte>(value));
                return offset;
            }

            // List32
            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Vbin32);
            offset += WireFormatting.WriteUInt32(seq.Slice(offset), (uint)len);
            offset += WireFormatting.Write(seq.Slice(offset), new ReadOnlySequence<byte>(value));
            return offset;
        }

        private static int WriteUInt16(Span<byte> seq, ushort value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Ushort);
            offset += WireFormatting.WriteUInt16(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteByte(Span<byte> seq, byte value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Ubyte);
            offset += WireFormatting.WriteByte(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteSByte(Span<byte> seq, sbyte value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Byte);
            offset += WireFormatting.WriteByte(seq.Slice(offset), (byte)value);
            return offset;
        }

        private static int WriteFloat(Span<byte> seq, float value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Float);
            var intFloat = BitConverter.SingleToInt32Bits(value);
            offset += WireFormatting.WriteInt32(seq.Slice(offset), intFloat);
            return offset;
        }

        private static int WriteDouble(Span<byte> seq, double value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Double);
            var intFloat = BitConverter.DoubleToInt64Bits(value);
            offset += WireFormatting.WriteInt64(seq.Slice(offset), intFloat);
            return offset;
        }

        private static int WriteIn16(Span<byte> seq, short value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Short);
            offset += WireFormatting.WriteInt16(seq.Slice(offset), value);
            return offset;
        }

        private static int WriteBool(Span<byte> seq, bool value)
        {
            return WireFormatting.WriteByte(seq, value ? FormatCode.BoolTrue : FormatCode.BoolFalse);
        }

        private static int WriteTimestamp(Span<byte> seq, DateTime value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Timestamp);
            var unixTime = ((DateTimeOffset)value).ToUnixTimeMilliseconds();
            offset += WireFormatting.WriteUInt64(seq.Slice(offset), (ulong)unixTime);
            return offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]

        // determinate the type size
        public static int GetSequenceSize(ReadOnlySequence<byte> data)
        {
            if (data.Length < 256)
            {
                return (int)data.Length +
                       1 + //marker 1 byte FormatCode.Vbin8
                       1; // (byte) data.Length
            }

            return (int)data.Length
                   + 1 //marker 1 byte  FormatCode.Vbin32 
                   + 4; // (uint) data.Length
        }

        private static int GetStringSize(string value)
        {
            return value.Length switch
            {
                0 => 1, // 0x40
                < 256 => value.Length + 1 + //marker 1 byte FormatCode.Vbin8
                         1,
                _ => value.Length + 1 //marker 1 byte  FormatCode.Vbin32 
                                  + 4
            };
        }

        private static int GetIntSize(int value)
        {
            if (value is < 128 and >= -128)
            {
                return 1 // FormatCode.SmallUint
                       + 1; //byte value
            }

            return 1 // FormatCode.Int
                   + 4; //byte value
        }

        private static int GetUIntSize(uint value)

        {
            return value switch
            {
                0 => 1, // FormatCode.Uint0

                < 256 => 1 //FormatCode.SmallUint
                         + 1, // uint byte value

                _ => 1 //FormatCode.SmallUint
                     + 4 // uint value
            };
        }

        private static int GetTimestampSize(DateTime value)
        {
            if (value == DateTime.MinValue)
            {
                return 1; // null
            }

            return 1 // FormatCode.Timestamp
                   + 8; // long unit time
        }

        private static int GetBytesSize(byte[] value)
        {
            return value.Length switch
            {
                0 => 1,
                // List8
                < 256 => 1 // FormatCode.Vbin8
                         + 1 // array len in byte
                         + value.Length,
                // List32
                _ => 1 // FormatCode.Vbin32
                     + 4 // array len in uint
                     + value.Length
            };
        }

        private static int GetInt64Size(long value)
        {
            if (value is < 128 and >= -128)
            {
                return 1 // FormatCode.Smalllong
                       + 1; //(byte) value
            }

            return 1 // FormatCode.Long
                   + 8; //value
        }

        private static int GetUInt64Size(ulong value)
        {
            return value switch
            {
                0 => 1,
                < 256 => 1 //FormatCode.SmallUlong 
                         + 1,
                _ => 1 //FormatCode.Ulong 
                     + 8
            };
        }

        private static int GetUInt16Size()
        {
            return 1 // FormatCode.Ushort
                   + 2; //value
        }

        public static int GetAnySize(object value)
        {
            return value switch
            {
                null => 1, //WriteNull(seq),
                string s => GetStringSize(s),
                int i => GetIntSize(i),
                uint u => GetUIntSize(u),
                ulong ul => GetUInt64Size(ul),
                long l => GetInt64Size(l),
                ushort => GetUInt16Size(),
                byte[] bArr => GetBytesSize(bArr),
                byte => 1,
                DateTime d => GetTimestampSize(d),
                _ => throw new AmqpParseException($"WriteAny Invalid type {value}")
            };
        }
    }
}
