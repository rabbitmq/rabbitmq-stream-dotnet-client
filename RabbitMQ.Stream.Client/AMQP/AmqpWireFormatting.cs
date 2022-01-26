using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace RabbitMQ.Stream.Client.AMQP
{
    public static class AmqpWireFormatting
    {
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

        private static int ReadString(ReadOnlySequence<byte> seq, out string value)
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

            throw new AMQP.AmqpParseException($"ReadString invalid type {type}");
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
        
        
        internal static int ReadBytes(ReadOnlySequence<byte> seq, out ReadOnlySequence<byte> value)
        {
            var offset = ReadType(seq, out var type);
            switch (type)
            {
                case AmqpType.TypeCodeVbin8:
                    offset += WireFormatting.ReadByte(seq.Slice(offset), out var length8);
                    value = seq.Slice(offset, length8);
                    return offset;
                case AmqpType.TypeCodeVbin32:
                    offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var length32);
                    value = seq.Slice(offset, length32);
                    return offset;
            }

            throw new AMQP.AmqpParseException($"ReadBytes Invalid type {type}");
        }
    }
}