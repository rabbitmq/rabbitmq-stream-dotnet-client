using System;
using System.Buffers;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;

namespace Tests
{
    public class Amqp10Tests
    {
        [Fact]
        [WaitTestBeforeAfter]
        public void ReadsThrowsExceptionInvalidType()
        {
            var data = new byte[10];
            data[0] = 0x64; // not a valid header  
            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadAny(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadBinary(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadLong(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadString(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadTimestamp(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadUInt64(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadCompositeHeader(new ReadOnlySequence<byte>(data), out var value,
                    out var next);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadListHeader(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadMapHeader(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadUint32(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadUByte(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadUshort(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadUint32(new ReadOnlySequence<byte>(data), out var value);
            });

            System.Diagnostics.Trace.WriteLine(" test passed");
        }


        [Fact]
        [WaitTestBeforeAfter]
        public void ValidateTypes()
        {
            ulong ulong0Value = 0x00;
            byte[] ulong0ValueBin = new byte[] {0x44};

            ulong ulongSmallValue = 0xf2;
            byte[] ulongSmallValueBin = new byte[] {0x53, 0xf2};

            ulong ulongValue = 0x12345678edcba098;
            byte[] ulongValueBin = new byte[] {0x80, 0x12, 0x34, 0x56, 0x78, 0xed, 0xcb, 0xa0, 0x98};

            // var array = new byte[8];
            // var arraySpan = new Span<byte>(array);
            // AmqpWireFormatting.WriteUInt64(arraySpan, ulongSmallValue);
            // var arrayRead = new ReadOnlySequence<byte>(arraySpan.ToArray());
            // AmqpWireFormatting.ReadUInt64(arrayRead, out var value);
            // Assert.Equal(value, ulongSmallValue);
            ValueTest(ulong0Value, ulong0ValueBin);
            ValueTest(ulongSmallValue, ulongSmallValueBin);
            ValueTest(ulongValue, ulongValueBin);
            
            byte ubyteValue = 0x33;
            byte[] ubyteValueBin = new byte[] { 0x50, 0x33 };

            ValueTest(ubyteValue, ubyteValueBin);
            
            ushort ushortValue = 0x1234;
            byte[] ushortValueBin = new byte[] { 0x60,  0x12, 0x34};
            
            ValueTest(ushortValue, ushortValueBin);

            
            uint uint0Value = 0x00;
            byte[] uint0ValueBin = new byte[] { 0x43 };

            uint uintSmallValue = 0xe1;
            byte[] uintSmallValueBin = new byte[] { 0x52, 0xe1 };

            uint uintValue = 0xedcba098;
            byte[] uintValueBin = new byte[] { 0x70, 0xed, 0xcb, 0xa0, 0x98 };

            ValueTest(uint0Value, uint0ValueBin);
            ValueTest(uintSmallValue, uintSmallValueBin);
            // ValueTest(uintValue, uintValueBin);

            
            
        }

        private static void ValueTest<T>(T value, byte[] result)
        {
            var array = new byte[result.Length];
            var arraySpan = new Span<byte>(array);
            AmqpWireFormatting.WriteAny(arraySpan, value);
            Assert.Equal(arraySpan.ToArray(), result);
            var arrayRead = new ReadOnlySequence<byte>(arraySpan.ToArray());
            AmqpWireFormatting.ReadAny(arrayRead, out var decodeValue);
            Assert.Equal(value, decodeValue);

        }
    }
}