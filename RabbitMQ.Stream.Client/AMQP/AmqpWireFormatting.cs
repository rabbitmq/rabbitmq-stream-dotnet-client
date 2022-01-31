using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static class AmqpWireFormatting
    {
        
        private static Encoding s_encoding = Encoding.UTF8;

        private static int ReadType(ReadOnlySequence<byte> seq, out byte value)
        {
            var read = WireFormatting.ReadByte(seq, out value);
            return read;
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
                    offset = ReadUbyte(seq, out var resultByte);
                    value = resultByte;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"can't read any {type}");
        }

        private static int ReadString(ReadOnlySequence<byte> seq, out string value)
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


        internal static int ReadMap(ReadOnlySequence<byte> seq, out Dictionary<string, string> dic)
        {
            dic = new Dictionary<string, string>();
            var offset = ReadMapHeader(seq, out var fields);
            for (var i = 0; i < fields; i++)
            {
                offset += AmqpWireFormatting.ReadAny(seq.Slice(offset), out var key);
                offset += AmqpWireFormatting.ReadAny(seq.Slice(offset), out var value);
                dic[(string) key] = (string) value;
            }

            return offset;
        }


        internal static int ReadUbyte(ReadOnlySequence<byte> seq, out byte value)
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


        internal static int ReadBytes(ReadOnlySequence<byte> seq, out ReadOnlySequence<byte> value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case FormatCode.Vbin8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var length8);
                    value = seq.Slice(offset, length8);
                    return offset;
                case FormatCode.Vbin32:
                    offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var length32);
                    value = seq.Slice(offset, length32);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadBytes Invalid type {type}");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if NETCOREAPP
        public static int GetByteCount(ReadOnlySpan<char> val) => val.IsEmpty ? 0 : s_encoding.GetByteCount(val);
#else
        public static int GetByteCount(string val) => string.IsNullOrEmpty(val) ? 0 : s_encoding.GetByteCount(val);
#endif

        public static int GetTableByteCount(IDictionary val)
        {
            if (val is null || val.Count == 0)
            {
                return 4;
            }

            int byteCount = 4;
            foreach (DictionaryEntry entry in val)
            {
                byteCount += GetByteCount(entry.Key.ToString()) + 1;
                byteCount += GetFieldValueByteCount(entry.Value);
            }

            return byteCount;
        }
        public static int GetTableByteCount(IDictionary<string, object> val)
        {
            if (val is null || val.Count == 0)
            {
                return 4;
            }

            int byteCount = 4;
            if (val is Dictionary<string, object> dict)
            {
                foreach (KeyValuePair<string, object> entry in dict)
                {
                    byteCount += GetByteCount(entry.Key) + 1;
                    byteCount += GetFieldValueByteCount(entry.Value);
                }
            }
            else
            {
                foreach (KeyValuePair<string, object> entry in val)
                {
                    byteCount += GetByteCount(entry.Key) + 1;
                    byteCount += GetFieldValueByteCount(entry.Value);
                }
            }

            return byteCount;
        }
        
        public static int GetFieldValueByteCount(object value)
        {
            // Order by likelihood of occurrence
            switch (value)
            {
                case null:
                    return 1;
                case string val:
                    return 5 + GetByteCount(val);
                case bool _:
                    return 2;
                case int _:
                case float _:
                    return 5;
                case byte[] val:
                    return 5 + val.Length;
                case IDictionary<string, object> val:
                    return 1 + GetTableByteCount(val);
                // case IList val:
                //     return 1 + GetArrayByteCount(val);
                // case AmqpTimestamp _:
                case double _:
                case long _:
                    return 9;
                case byte _:
                case sbyte _:
                    return 2;
                case short _:
                    return 3;
                case uint _:
                    return 5;
                case decimal _:
                    return 6;
                case IDictionary val:
                    return 1 + GetTableByteCount(val);
                //TODO wire
                // case BinaryTableValue val:
                //     return 5 + val.Bytes.Length;
                 default:
                return 0;
                //     return ThrowInvalidTableValue(value);
            }
        }
    }
}