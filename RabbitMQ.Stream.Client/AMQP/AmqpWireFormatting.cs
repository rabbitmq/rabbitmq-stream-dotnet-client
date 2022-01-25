using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static class AmqpWireFormatting
    {
        private static int ReadType(ReadOnlySequence<byte> seq, out AmqpType type)
        {
            var read = WireFormatting.ReadByte(seq, out var value);
            type = (AmqpType) value;
            return read;
        }


        internal static int ReadAny(ReadOnlySequence<byte> seq, out object value)
        {
            var offset = 0;
            ReadType(seq, out var type);
            switch (type)
            {
                case AmqpType.TypeCodeSym32:
                case AmqpType.TypeCodeSym8:
                case AmqpType.TypeCodeStr8:
                case AmqpType.TypeCodeStr32:

                    offset = ReadString(seq, out var resultString);
                    value = resultString;
                    return offset;
                case AmqpType.TypeCodeUbyte:
                    offset = ReadUbyte(seq, out var resultByte);
                    value = resultByte;
                    return offset;
            }

            throw new AMQP.AmqpParseException($"can't read any {type}");
        }

        internal static int ReadString(ReadOnlySequence<byte> seq, out string value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case AmqpType.TypeCodeSym32:
                case AmqpType.TypeCodeSym8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var lenC);
                    value = Encoding.UTF8.GetString(seq.Slice(offset, lenC));
                    return offset + lenC;

                case AmqpType.TypeCodeStr8:
                case AmqpType.TypeCodeStr32:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var len);
                    value = Encoding.UTF8.GetString(seq.Slice(offset, len));
                    return offset + len;
            }

            value = "";
            return offset;
        }


        internal static int ReadMapHeader(ReadOnlySequence<byte> seq, out uint count)
        {
            var offset = ReadType(seq, out var type);

            switch (type)
            {
                case AmqpType.TypeCodeMap8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var first);
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var size);
                    count = size;
                    return offset;
            }
            
            throw new AMQP.AmqpParseException($"ReadMapHeader Invalid type {type}");
        }


        internal static int ReadUbyte(ReadOnlySequence<byte> seq, out byte value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case AmqpType.TypeCodeUbyte:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out value);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadUbyte Invalid type {type}");
        }
    }
}