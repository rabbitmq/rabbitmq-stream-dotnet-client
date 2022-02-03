#nullable enable
using System;
using System.Buffers;
using System.Collections;
using System.Runtime.CompilerServices;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static partial class AmqpWireFormatting
    {
        public static int WriteData(Span<byte> seq, ReadOnlySequence<byte> data)
        {
            var offset = 0;
            if (data.Length < 256)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Vbin8); //binary marker
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte) data.Length); //length
            }
            else
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Vbin32); //binary marker
                offset += WireFormatting.WriteUInt32(seq.Slice(offset), (uint) data.Length); //length
            }

            offset += WireFormatting.Write(seq.Slice(offset), data);
            return offset;
        }

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
                byte[] bArr => bArr.Length == 0 ? WriteNull(seq) : WriteBytes(seq, bArr),
                DateTime d => d == DateTime.MinValue ? WriteNull(seq) : WriteTimestamp(seq, d),
                _ => throw new AMQP.AmqpParseException($"WriteAny Invalid type {value}")
            };

            ;
        }

        public static int WriteString(Span<byte> seq, string value)
        {
            var len = value.Length;
            var offset = 0;
            // Str8
            if (len < 256)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Str8);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte) len);
                offset += s_encoding.GetBytes(value, seq.Slice(offset));
                return offset;
            }

            // Str32
            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Str32);
            offset += WireFormatting.WriteInt32(seq.Slice(offset), len);
            offset += s_encoding.GetBytes(value, seq.Slice(offset));
            return offset;
        }


        public static int WriteUInt64(Span<byte> seq, ulong value)
        {
            if (value == 0)
            {
                return WireFormatting.WriteByte(seq, FormatCode.Ulong0);
            }

            var offset = 0;
            if (value < 256)
            {
                offset += WireFormatting.WriteByte(seq, FormatCode.SmallUlong);
                offset += WireFormatting.WriteByte(seq, (byte) value);
                return offset;
            }

            offset += WireFormatting.WriteByte(seq, FormatCode.Ulong);
            offset += WireFormatting.WriteUInt64(seq, value);
            return offset;
        }

        public static int WriteUInt(Span<byte> seq, uint value)

        {
            var offset = 0;
            switch (value)
            {
                case 0:
                    return WireFormatting.WriteByte(seq, FormatCode.Uint0);
                case < 256:
                    offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.SmallUint);
                    offset += WireFormatting.WriteUInt32(seq.Slice(offset), value);
                    return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Uint);
            offset += WireFormatting.WriteUInt32(seq.Slice(offset), value);
            return offset;
        }

        public static int WriteInt(Span<byte> seq, int value)
        {
            var offset = 0;
            if (value is < 128 and >= -128)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.SmallUint);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte) value);

                return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Int);
            offset += WireFormatting.WriteInt32(seq.Slice(offset), value);
            return offset;
        }

        public static int WriteInt64(Span<byte> seq, long value)
        {
            var offset = 0;
            if (value is < 128 and >= -128)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Smalllong);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte) value);

                return offset;
            }

            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Long);
            offset += WireFormatting.WriteInt64(seq.Slice(offset), value);
            return offset;
        }

        public static int WriteNull(Span<byte> seq)
        {
            return WireFormatting.WriteByte(seq, FormatCode.Null);
        }


        public static int WriteBytes(Span<byte> seq, byte[] value)
        {
            var len = value.Length;
            var offset = 0;

            // List8
            if (len < 256)
            {
                offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Vbin8);
                offset += WireFormatting.WriteByte(seq.Slice(offset), (byte) len);
                offset += WireFormatting.WriteBytes(seq.Slice(offset), new ReadOnlySequence<byte>(value));
                return offset;
            }

            // List32
            offset += WireFormatting.WriteByte(seq.Slice(offset), FormatCode.Vbin32);
            offset += WireFormatting.WriteUInt32(seq.Slice(offset), (uint) len);
            offset += WireFormatting.WriteBytes(seq.Slice(offset), new ReadOnlySequence<byte>(value));
            return offset;
        }

        public static int WriteUInt16(Span<byte> seq, ushort value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Ushort);
            offset += WireFormatting.WriteUInt16(seq, value);
            return offset;
        }


        public static int WriteTimestamp(Span<byte> seq, DateTime value)
        {
            var offset = WireFormatting.WriteByte(seq, FormatCode.Timestamp);
            var unixTime = ((DateTimeOffset) value).ToUnixTimeSeconds();
            offset += WireFormatting.WriteUInt64(seq.Slice(offset), (ulong) unixTime);
            return offset;
        }


        // determinate the type size
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetSequenceSize(ReadOnlySequence<byte> data)
        {
            if (data.Length < 256)
                return (int) data.Length +
                       1 + //marker 1 byte FormatCode.Vbin8
                       1; // (byte) data.Length
            return (int) data.Length
                   + 1 //marker 1 byte  FormatCode.Vbin32 
                   + 4; // (uint) data.Length
        }


        public static int GetStringSize(string value)
        {
            return value.Length switch
            {
                0 => 1,
                < 256 => value.Length + 1 + //marker 1 byte FormatCode.Vbin8
                         1,
                _ => value.Length + 1 //marker 1 byte  FormatCode.Vbin32 
                                  + 4
            };
        }

        public static int GetIntSize(int value)
        {
            if (value is < 128 and >= -128)
            {
                return 1 // FormatCode.SmallUint
                       + 1; //byte value
            }

            return 1 // FormatCode.Int
                   + 4; //byte value
        }


        public static int GetUIntSize(uint value)

        {
            return value switch
            {
                0 => 1, // FormatCode.Uint0

                _ => 1 //FormatCode.SmallUint
                     + 4 // uint value
            };
        }


        public static int GetTimestampSize(DateTime value)
        {
            if (value == DateTime.MinValue) return 1; // null
            return 1 // FormatCode.Timestamp
                   + 8; // long unit time
        }

        public static int GetBytesSize(byte[] value)
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


        public static int GetInt64Size(long value)
        {
            if (value is < 128 and >= -128)
            {
                return 1 // FormatCode.Smalllong
                       + 1; //(byte) value
            }

            return 1 // FormatCode.Long
                   + 8; //value
        }


        public static int GetUInt64Size(ulong value)
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


        public static int GetUInt16Size(ushort value)
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
                ushort ush => GetUInt16Size(ush),
                byte[] bArr => GetBytesSize(bArr),
                byte => 1,
                DateTime d => GetTimestampSize(d),
                _ => throw new AMQP.AmqpParseException($"WriteAny Invalid type {value}")
            };
        }
    }
}