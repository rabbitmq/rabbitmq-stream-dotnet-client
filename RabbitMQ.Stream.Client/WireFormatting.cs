using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using System.Linq;

namespace RabbitMQ.Stream.Client
{
    internal static class WireFormatting
    {
        static WireFormatting()
        {
        }

        internal static int StringSize(string s)
        {
            if(String.IsNullOrEmpty(s))
                return 2;
            //TODO: can this be done without allocation?
            return 2 + Encoding.UTF8.GetBytes(s).Length;
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
                WriteUInt32(span, (uint) msg.Length);
                Write(span.Slice(4), msg);
                return 4 + (int) msg.Length;
        }
        
        internal static int WriteString(Span<byte> span, string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return WriteUInt16(span, 0);
            }
            // I'm sure there are better ways
            Span<byte> stringBytes = Encoding.UTF8.GetBytes(s);
            WriteUInt16(span, (ushort) stringBytes.Length);
            stringBytes.CopyTo(span.Slice(2));
            return stringBytes.Length + 2;
        }
        
        internal static int ReadUInt32(ReadOnlySequence<byte> seq, out uint value)
        {
            value = BinaryPrimitives.ReadUInt32BigEndian(AsSpan(seq, 4));
            return 4;
        }
        internal static int ReadInt32(ReadOnlySequence<byte> seq, out int value)
        {
            value = BinaryPrimitives.ReadInt32BigEndian(AsSpan(seq, 4));
            return 4;
        }
        internal static int ReadUInt16(ReadOnlySequence<byte> seq, out ushort value)
        {
            value = BinaryPrimitives.ReadUInt16BigEndian(AsSpan(seq, 2));
            return 2;
        }

        internal static int ReadUInt64(ReadOnlySequence<byte> seq, out ulong value)
        {
            value = BinaryPrimitives.ReadUInt64BigEndian(AsSpan(seq, 8));
            return 8;
        }
        private static ReadOnlySpan<byte> AsSpan(ReadOnlySequence<byte> seq, int len)
        {
            var s = seq.Slice(0, len);   
            return s.IsSingleSegment ? s.FirstSpan : s.ToArray();
        }
        internal static int ReadByte(ReadOnlySequence<byte> seq, out byte b)
        {
                b = seq.FirstSpan[0];
                return 1;
        }

        internal static int ReadString(ReadOnlySequence<byte> s, out string k)
        {
            ReadUInt16(s, out var len);
            k = Encoding.UTF8.GetString(s.Slice(2, len));
            return len + 2;
        }

        internal static int ReadBytes(ReadOnlySequence<byte> seq, out byte[] data)
        {
            var len = BinaryPrimitives.ReadUInt32BigEndian(AsSpan(seq, 4));
            data = seq.Slice(4, len).ToArray();
            return 4 + (int) len;
        }

        internal static int ReadInt64(ReadOnlySequence<byte> seq, out long value)
        {
            value = BinaryPrimitives.ReadInt64BigEndian(AsSpan(seq, 8));
            return 8;
        }
    }
}