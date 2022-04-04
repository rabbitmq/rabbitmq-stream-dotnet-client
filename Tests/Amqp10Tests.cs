// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;

namespace Tests
{
    public class Amqp10Tests
    {
        [Fact]
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
                AmqpWireFormatting.ReadInt64(new ReadOnlySequence<byte>(data), out var value);
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

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadInt32(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadFloat(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadDouble(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadSByte(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadBool(new ReadOnlySequence<byte>(data), out var value);
            });

            Assert.Throws<AmqpParseException>(() =>
            {
                AmqpWireFormatting.ReadInt16(new ReadOnlySequence<byte>(data), out var value);
            });

            System.Diagnostics.Trace.WriteLine(" test passed");
        }

        [Fact]
        public void ValidateFormatCode()
        {
            const bool boolTrue = true;
            var boolTrueBin = new byte[] { 0x41 };
            RunValidateFormatCode(boolTrue, boolTrueBin);

            const bool boolFalse = false;
            var boolFalseBin = new byte[] { 0x42 };
            RunValidateFormatCode(boolFalse, boolFalseBin);

            const ulong ulong0Value = 0x00;
            var ulong0ValueBin = new byte[] { 0x44 };

            const ulong ulongSmallValue = 0xf2;
            var ulongSmallValueBin = new byte[] { 0x53, 0xf2 };

            const ulong ulongValue = 0x12345678edcba098;
            var ulongValueBin = new byte[] { 0x80, 0x12, 0x34, 0x56, 0x78, 0xed, 0xcb, 0xa0, 0x98 };

            RunValidateFormatCode(ulong0Value, ulong0ValueBin);
            RunValidateFormatCode(ulongSmallValue, ulongSmallValueBin);
            RunValidateFormatCode(ulongValue, ulongValueBin);

            const byte ubyteValue = 0x33;
            var ubyteValueBin = new byte[] { 0x50, 0x33 };

            RunValidateFormatCode(ubyteValue, ubyteValueBin);

            const ushort ushortValue = 0x1234;
            var ushortValueBin = new byte[] { 0x60, 0x12, 0x34 };

            RunValidateFormatCode(ushortValue, ushortValueBin);

            const uint uint0Value = 0x00;
            var uint0ValueBin = new byte[] { 0x43 };

            const uint uintSmallValue = 0xe1;
            var uintSmallValueBin = new byte[] { 0x52, 0xe1 };

            const uint uintValue = 0xedcba098;
            var uintValueBin = new byte[] { 0x70, 0xed, 0xcb, 0xa0, 0x98 };

            RunValidateFormatCode(uint0Value, uint0ValueBin);
            RunValidateFormatCode(uintSmallValue, uintSmallValueBin);
            RunValidateFormatCode(uintValue, uintValueBin);

            const sbyte byteValue = -20;
            var byteValueBin = new byte[] { 0x51, 0xec };

            RunValidateFormatCode(byteValue, byteValueBin);

            const short shortValue = 0x5678;
            var shortValueBin = new byte[] { 0x61, 0x56, 0x78 };
            RunValidateFormatCode(shortValue, shortValueBin);

            // int intSmallValue = -77;
            // byte[] intSmallValueBin = new byte[] {0x54, 0xb3};
            // ValueTest(intSmallValue, intSmallValueBin);
            // //TODO Need to write another kind of the test since the intSmallValue is cast to byte so = 0xb3

            const int intValue = 0x56789a00;
            var intValueBin = new byte[] { 0x71, 0x56, 0x78, 0x9a, 0x00 };
            RunValidateFormatCode(intValue, intValueBin);

            const long longValue64 = -111111111111; //FFFFFFE62142FE39
            var longValueBin64 = new byte[] { 0x81, 0xff, 0xff, 0xff, 0xe6, 0x21, 0x42, 0xfe, 0x39 };
            RunValidateFormatCode(longValue64, longValueBin64);

            const long longValue8 = 127;
            var longValueBin8 = new byte[] { 0x55, 0x7F };
            RunValidateFormatCode(longValue8, longValueBin8);

            const float floatValue = -88.88f;
            var floatValueBin = new byte[] { 0x72, 0xc2, 0xb1, 0xc2, 0x8f };
            RunValidateFormatCode(floatValue, floatValueBin);

            var dtValue = DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime();
            var dtValueBin = new byte[] { 0x83, 0x00, 0x00, 0x01, 0x1d, 0x59, 0x8d, 0x1e, 0xa0 };
            RunValidateFormatCode(dtValue, dtValueBin);

            const double doubleValue = 111111111111111.22222222222;
            var doubleValueBin = new byte[] { 0x82, 0x42, 0xd9, 0x43, 0x84, 0x93, 0xbc, 0x71, 0xce };
            RunValidateFormatCode(doubleValue, doubleValueBin);

            const string str8Value = "amqp";
            var str8Utf8ValueBin = new byte[] { 0xa1, 0x04, 0x61, 0x6d, 0x71, 0x70 };
            RunValidateFormatCode(str8Value, str8Utf8ValueBin);

            var str32Value = "";
            var str32Utf8ValueBin = new byte[290 + 1 + 4];
            str32Utf8ValueBin[0] = 0xb1;
            str32Utf8ValueBin[1] = 0x0;
            str32Utf8ValueBin[2] = 0x0;
            str32Utf8ValueBin[3] = 0x1;
            str32Utf8ValueBin[4] = 0x22;
            for (var i = 0; i < 290; i++)
            {
                str32Value += "a";
                str32Utf8ValueBin[i + 5] = 0x61;
            }

            RunValidateFormatCode(str32Value, str32Utf8ValueBin);

            var bin8Value = new byte[56];
            var bin32Value = new byte[500]; //
            var bin8ValueBin = new byte[1 + 1 + 56];
            var bin32ValueBin = new byte[1 + 4 + 500]; //
            RunValidateFormatCode(bin32Value, bin32ValueBin);
            RunValidateFormatCode(bin8Value, bin8ValueBin);

            // Guid uuidValue = new Guid("f275ea5e-0c57-4ad7-b11a-b20c563d3b71");
            // byte[] uuidValueBin = new byte[] { 0x98, 0xf2, 0x75, 0xea, 0x5e, 0x0c, 0x57, 0x4a, 0xd7, 0xb1, 0x1a, 0xb2, 0x0c, 0x56, 0x3d, 0x3b, 0x71 };
            // ValueTest(uuidValue, uuidValueBin);
        }

        private static void RunValidateFormatCode<T>(T value, byte[] result)
        {
            var array = new byte[result.Length];
            var arraySpan = new Span<byte>(array);
            AmqpWireFormatting.WriteAny(arraySpan, value);
            var arrayRead = new ReadOnlySequence<byte>(arraySpan.ToArray());
            AmqpWireFormatting.ReadAny(arrayRead, out var decodeValue);
            if (typeof(T) == typeof(byte[]))
            {
                var b1 = (byte[])(object)value;
                var b2 = (byte[])(object)decodeValue;
                Assert.Equal(b1, b2);
            }
            else
            {
                Assert.Equal(arraySpan.ToArray(), result);
                Assert.Equal(value, decodeValue);
            }
        }

        [Fact]
        public void Validate32Bytes8BytesLists()
        {
            var value32Bin = new byte[] { 0xD0, 0x0, 0x0, 0x0, 0xF, 0x0, 0x0, 0x1, 0xF };
            AmqpWireFormatting.ReadListHeader(new ReadOnlySequence<byte>(value32Bin), out var len32);
            Assert.Equal(271, len32);

            var value8Bin = new byte[] { 0xc0, 0xF, 0xF0 };
            AmqpWireFormatting.ReadListHeader(new ReadOnlySequence<byte>(value8Bin), out var len8);
            Assert.Equal(240, len8);

            var value0Bin = new byte[] { 0x45 };
            AmqpWireFormatting.ReadListHeader(new ReadOnlySequence<byte>(value0Bin), out var len0);
            Assert.Equal(0, len0);

            var valueComposite8Bin = new byte[] { 0x0, 0x53, 0x73, 0xc0, 0xF, 0xF0 };
            AmqpWireFormatting.ReadCompositeHeader(new ReadOnlySequence<byte>(valueComposite8Bin),
                out var compositeLen32, out _);
            Assert.Equal(240, compositeLen32);
        }

        [Fact]
        public void ValidateMessagesFromGo()
        {
            // These files are generated with the Go AMQP 1.0 client
            // The idea is to have an external validation for the messages
            // see: https://github.com/rabbitmq/rabbitmq-stream-go-client/tree/main/generate
            // dump messages are included as resources.
            // remove these tests at some point ?? 
            //  body len < 250 bytes
            var body250 = SystemUtils.GetFileContent("message_body_250");
            var msg250 = Message.From(new ReadOnlySequence<byte>(body250));
            Assert.NotNull(msg250);
            Assert.True(msg250.Data.Size <= byte.MaxValue);
            Assert.Null(msg250.ApplicationProperties);
            Assert.Null(msg250.Properties);

            //  body len > 256 bytes important to read the int in different way 
            //  body len >= 700 bytes 
            var body700 = SystemUtils.GetFileContent("message_body_700");
            var msg700 = Message.From(new ReadOnlySequence<byte>(body700));
            Assert.NotNull(msg700);
            Assert.True(msg700.Data.Size > byte.MaxValue);
            Assert.Null(msg700.Properties);

            //  body len >= 300 bytes 
            // also the ApplicationProperties are not null
            var prop300 = SystemUtils.GetFileContent("message_random_application_properties_300");
            var msgProp300 = Message.From(new ReadOnlySequence<byte>(prop300));
            Assert.NotNull(msgProp300);
            Assert.True(msgProp300.Data.Size > byte.MaxValue);
            Assert.NotNull(msgProp300.ApplicationProperties);

            //  body len >= 500 bytes 
            // also the ApplicationProperties are not null
            var prop500 = SystemUtils.GetFileContent("message_random_application_properties_500");
            var msgProp500 = Message.From(new ReadOnlySequence<byte>(prop500));
            Assert.NotNull(msgProp500);
            Assert.NotNull(msgProp500.ApplicationProperties);

            //  body len >= 900 bytes 
            // ApplicationProperties are not null
            // Properties is not null
            var prop900 = SystemUtils.GetFileContent("message_random_application_properties_properties_900");
            var msgProp900 = Message.From(new ReadOnlySequence<byte>(prop900));
            Assert.NotNull(msgProp900);
            Assert.NotNull(msgProp900.ApplicationProperties);
            foreach (var (key, value) in msgProp900.ApplicationProperties)
            {
                Assert.True(((string)key).Length >= 900);
                Assert.True(((string)value).Length >= 900);
            }

            Assert.NotNull(msgProp900.Properties);
            Assert.True(!string.IsNullOrEmpty(msgProp900.Properties.ReplyTo));
            Assert.True(!string.IsNullOrEmpty(msgProp900.Properties.ContentEncoding));
            Assert.True(!string.IsNullOrEmpty(msgProp900.Properties.ContentType));
            Assert.True(!string.IsNullOrEmpty(msgProp900.Properties.GroupId));
            Assert.Equal((ulong)33333333, msgProp900.Properties.MessageId);
            Assert.Equal("json", msgProp900.Properties.ContentType);
            Assert.Equal("myCoding", msgProp900.Properties.ContentEncoding);
            Assert.Equal((uint)10, msgProp900.Properties.GroupSequence);
            Assert.True(msgProp900.Properties.CreationTime != DateTime.MinValue);
            Assert.True(msgProp900.Properties.AbsoluteExpiryTime != DateTime.MinValue);

            // Test message to check if all the fields with "test" value
            var staticTest = SystemUtils.GetFileContent("static_test_message_compare");
            var msgStaticTest = Message.From(new ReadOnlySequence<byte>(staticTest));
            Assert.NotNull(msgStaticTest);
            Assert.Equal("test", Encoding.Default.GetString(msgStaticTest.Data.Contents.ToArray()));
            Assert.Equal("test", msgStaticTest.Properties.Subject);
            Assert.Equal("test", msgStaticTest.Properties.MessageId);
            Assert.Equal("test", msgStaticTest.Properties.ReplyTo);
            Assert.Equal("test", msgStaticTest.Properties.ContentType);
            Assert.Equal("test", msgStaticTest.Properties.ContentEncoding);
            Assert.Equal("test", msgStaticTest.Properties.GroupId);
            Assert.Equal("test", msgStaticTest.Properties.ReplyToGroupId);
            Assert.Equal("test", Encoding.Default.GetString(msgStaticTest.Properties.UserId));
            foreach (var (key, value) in msgStaticTest.ApplicationProperties)
            {
                Assert.Equal("test", key);
                Assert.Equal("test", value);
            }

            Assert.Equal("test", msgStaticTest.Annotations["test"]);
            Assert.Equal((long)1, msgStaticTest.Annotations[(long)1]);
            Assert.Equal((long)100_000, msgStaticTest.Annotations[(long)100_000]);

            var header = SystemUtils.GetFileContent("header_amqpvalue_message");
            var msgHeader = Message.From(new ReadOnlySequence<byte>(header));
            Assert.NotNull(msgHeader);
            Assert.NotNull(msgHeader.MessageHeader);
            Assert.NotNull(msgHeader.AmqpValue);
            Assert.True(msgHeader.MessageHeader.Durable);
            Assert.True(msgHeader.MessageHeader.FirstAcquirer);
            Assert.Equal(100, msgHeader.MessageHeader.Priority);
            Assert.Equal((uint)300, msgHeader.MessageHeader.DeliveryCount);
            Assert.True(msgHeader.MessageHeader.Ttl == 0);
            Assert.Equal("amqpValue", msgHeader.AmqpValue);
        }

        [Fact]
        public void MapEntriesWithAnEmptyKeyShouldNotBeWrittenToTheWire()
        {
            // Given we have an annotation with a valid key
            var annotation = new Annotations();
            annotation.Add("valid key", "");

            var expectedMapSize = annotation.Size;
            var array = new byte[expectedMapSize];
            var arraySpan = new Span<byte>(array);

            Assert.Equal(expectedMapSize, annotation.Write(arraySpan));

            // when we add a empty key and write the annotation again
            annotation.Add("", "");
            arraySpan.Clear();
            var actualMapSize = annotation.Write(arraySpan);

            // we do not expect the new entry to be written
            Assert.Equal(expectedMapSize, actualMapSize);
        }
    }
}
