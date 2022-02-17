// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

namespace RabbitMQ.Stream.Client.AMQP
{
    public static class FormatCode
    {
        public const byte Described = 0x00;
        public const byte Null = 0x40;

        // Bool
        public const byte Bool = 0x56; // boolean with the octet 0x00 being false and octet 0x01 being true
        public const byte BoolTrue = 0x41;
        public const byte BoolFalse = 0x42;

        // Unsigned
        public const byte Ubyte = 0x50; // 8-bit unsigned integer (1)
        public const byte Ushort = 0x60; // 16-bit unsigned integer in network byte order (2)
        public const byte Uint = 0x70; // 32-bit unsigned integer in network byte order (4)
        public const byte SmallUint = 0x52; // unsigned integer value in the range 0 to 255 inclusive (1)
        public const byte Uint0 = 0x43; // the uint value 0 (0)
        public const byte Ulong = 0x80; // 64-bit unsigned integer in network byte order (8)
        public const byte SmallUlong = 0x53; // unsigned long value in the range 0 to 255 inclusive (1)
        public const byte Ulong0 = 0x44; // the ulong value 0 (0)

        // Signed
        public const byte Byte = 0x51; // 8-bit two's-complement integer (1)
        public const byte Short = 0x61; // 16-bit two's-complement integer in network byte order (2)
        public const byte Int = 0x71; // 32-bit two's-complement integer in network byte order (4)
        public const byte Smallint = 0x54; // 8-bit two's-complement integer (1)
        public const byte Long = 0x81; // 64-bit two's-complement integer in network byte order (8)
        public const byte Smalllong = 0x55; // 8-bit two's-complement integer

        // Decimal
        public const byte Float = 0x72; // IEEE 754-2008 binary32 (4)
        public const byte Double = 0x82; // IEEE 754-2008 binary64 (8)

        public const byte
            Decimal32 = 0x74; // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)

        public const byte
            Decimal64 = 0x84; // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)

        public const byte
            Decimal128 = 0x94; // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)

        public const byte Vbin8 = 0xa0; // up to 2^8 - 1 octets of binary data (1 + variable)
        public const byte Vbin32 = 0xb0; // up to 2^32 - 1 octets of binary data (4 + variable)

        public const byte
            Str8 = 0xa1; // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)

        public const byte
            Str32 =
                0xb1; // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)

        public const byte
            Sym8 =
                0xa3; // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)

        public const byte
            Sym32 =
                0xb3; // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)

        // Compound
        public const byte List0 = 0x45; // the empty list (i.e. the list with no elements) (0)

        public const byte
            List8 = 0xc0; // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)

        public const byte
            List32 = 0xd0; // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)

        public const byte Map8 = 0xc1; // up to 2^8 - 1 octets of encoded map data (1 + compound)
        public const byte Map32 = 0xd1; // up to 2^32 - 1 octets of encoded map data (4 + compound)

        public const byte
            Array8 = 0xe0; // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)

        public const byte
            Array32 = 0xf0; // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)

        public const byte Char = 0x73; // a UTF-32BE encoded Unicode character (4)

        public const byte Timestamp
            = 0x83; // 64-bit two's-complement integer representing milliseconds since the unix epoch

        public const byte
            UUID = 0x98; // UUID as defined in section 4.1.2 of RFC-4122
    }
}
