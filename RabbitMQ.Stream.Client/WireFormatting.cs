// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Linq;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    internal static class WireFormatting
    {
        private static readonly Encoding s_encoding = Encoding.UTF8;

        internal static int StringSize(string @string)
        {
            return string.IsNullOrEmpty(@string) ? 2 : 2 + s_encoding.GetByteCount(@string);
        }

        internal static int WriteByte(Span<byte> span, byte value)
        {
            span[0] = value;
            return 1;
        }

        internal static int WriteInt32(Span<byte> span, int value)
        {
            BinaryPrimitives.WriteInt32BigEndian(span, value);
            return 4;
        }

        internal static int WriteUInt16(Span<byte> p, ushort value)
        {
            BinaryPrimitives.WriteUInt16BigEndian(p, value);
            return 2;
        }

        internal static int WriteInt16(Span<byte> p, short value)
        {
            BinaryPrimitives.WriteInt16BigEndian(p, value);
            return 2;
        }

        internal static int WriteUInt64(Span<byte> span, ulong value)
        {
            BinaryPrimitives.WriteUInt64BigEndian(span, value);
            return 8;
        }
        internal static int WriteInt64(Span<byte> span, long value)
        {
            BinaryPrimitives.WriteInt64BigEndian(span, value);
            return 8;
        }
        internal static int WriteUInt32(Span<byte> span, uint value)
        {
            BinaryPrimitives.WriteUInt32BigEndian(span, value);
            return 4;
        }

        internal static int Write(Span<byte> span, ReadOnlySequence<byte> msg)
        {
            msg.CopyTo(span);
            return (int)msg.Length;
        }

        internal static int WriteBytes(Span<byte> span, ReadOnlySequence<byte> msg)
        {
            WriteUInt32(span, (uint)msg.Length);
            Write(span.Slice(4), msg);
            return 4 + (int)msg.Length;
        }

        internal static int WriteString(Span<byte> span, string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return WriteUInt16(span, 0);
            }

            // I'm sure there are better ways
            var bytecount = s_encoding.GetBytes(s, span.Slice(2));
            WriteUInt16(span, (ushort)bytecount);
            return 2 + bytecount;
        }

        internal static int ReadUInt32(ReadOnlySequence<byte> seq, out uint value)
        {
            if (seq.FirstSpan.Length >= 4)
            {
                value = BinaryPrimitives.ReadUInt32BigEndian(seq.FirstSpan);
            }
            else
            {
                Span<byte> tempSpan = stackalloc byte[4];
                seq.Slice(0, 4).CopyTo(tempSpan);
                value = BinaryPrimitives.ReadUInt32BigEndian(tempSpan);
            }

            return 4;
        }
        internal static int ReadInt32(ReadOnlySequence<byte> seq, out int value)
        {
            if (seq.FirstSpan.Length >= 4)
            {
                value = BinaryPrimitives.ReadInt32BigEndian(seq.FirstSpan);
            }
            else
            {
                Span<byte> tempSpan = stackalloc byte[4];
                seq.Slice(0, 4).CopyTo(tempSpan);
                value = BinaryPrimitives.ReadInt32BigEndian(tempSpan);
            }

            return 4;
        }
        internal static int ReadUInt16(ReadOnlySequence<byte> seq, out ushort value)
        {
            if (seq.FirstSpan.Length >= 2)
            {
                value = BinaryPrimitives.ReadUInt16BigEndian(seq.FirstSpan);
            }
            else
            {
                Span<byte> tempSpan = stackalloc byte[2];
                seq.Slice(0, 2).CopyTo(tempSpan);
                value = BinaryPrimitives.ReadUInt16BigEndian(tempSpan);
            }

            return 2;
        }

        internal static int ReadInt16(ReadOnlySequence<byte> seq, out short value)
        {
            if (seq.FirstSpan.Length >= 2)
            {
                value = BinaryPrimitives.ReadInt16BigEndian(seq.FirstSpan);
            }
            else
            {
                Span<byte> tempSpan = stackalloc byte[2];
                seq.Slice(0, 2).CopyTo(tempSpan);
                value = BinaryPrimitives.ReadInt16BigEndian(tempSpan);
            }

            return 2;
        }

        internal static int ReadUInt64(ReadOnlySequence<byte> seq, out ulong value)
        {
            if (seq.FirstSpan.Length >= 8)
            {
                value = BinaryPrimitives.ReadUInt64BigEndian(seq.FirstSpan);
            }
            else
            {
                Span<byte> tempSpan = stackalloc byte[8];
                seq.Slice(0, 8).CopyTo(tempSpan);
                value = BinaryPrimitives.ReadUInt64BigEndian(tempSpan);
            }

            return 8;
        }

        internal static int ReadByte(ReadOnlySequence<byte> seq, out byte b)
        {
            b = seq.FirstSpan[0];
            return 1;
        }

        internal static int ReadString(ReadOnlySequence<byte> s, out string k)
        {
            ReadUInt16(s, out var len);
            k = s_encoding.GetString(s.Slice(2, len));
            return len + 2;
        }

        internal static int ReadBytes(ReadOnlySequence<byte> seq, out byte[] data)
        {
            ReadUInt32(seq, out var len);
            data = seq.Slice(4, len).ToArray();
            return 4 + (int)len;
        }

        internal static int ReadBytes(ReadOnlySequence<byte> seq, int len, out byte[] data)
        {
            data = seq.Slice(0, len).ToArray();
            return len;
        }

        internal static int ReadInt64(ReadOnlySequence<byte> seq, out long value)
        {
            if (seq.FirstSpan.Length >= 8)
            {
                value = BinaryPrimitives.ReadInt64BigEndian(seq.FirstSpan);
            }
            else
            {
                Span<byte> tempSpan = stackalloc byte[8];
                seq.Slice(0, 8).CopyTo(tempSpan);
                value = BinaryPrimitives.ReadInt64BigEndian(tempSpan);
            }

            return 8;
        }
    }
}
