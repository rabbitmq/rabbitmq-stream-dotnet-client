namespace RabbitMQ.Stream.Client.AMQP
{
    public enum AmqpType
    {
        TypeCodeNull = 0x40,

        // Bool
        TypeCodeBool = 0x56, // boolean with the octet 0x00 being false and octet 0x01 being true
        TypeCodeBoolTrue = 0x41,
        TypeCodeBoolFalse = 0x42,

        // Unsigned
        TypeCodeUbyte = 0x50, // 8-bit unsigned integer (1)
        TypeCodeUshort = 0x60, // 16-bit unsigned integer in network byte order (2)
        TypeCodeUint = 0x70, // 32-bit unsigned integer in network byte order (4)
        TypeCodeSmallUint = 0x52, // unsigned integer value in the range 0 to 255 inclusive (1)
        TypeCodeUint0 = 0x43, // the uint value 0 (0)
        TypeCodeUlong = 0x80, // 64-bit unsigned integer in network byte order (8)
        TypeCodeSmallUlong = 0x53, // unsigned long value in the range 0 to 255 inclusive (1)
        TypeCodeUlong0 = 0x44, // the ulong value 0 (0)

        // Signed
        TypeCodeByte = 0x51, // 8-bit two's-complement integer (1)
        TypeCodeShort = 0x61, // 16-bit two's-complement integer in network byte order (2)
        TypeCodeInt = 0x71, // 32-bit two's-complement integer in network byte order (4)
        TypeCodeSmallint = 0x54, // 8-bit two's-complement integer (1)
        TypeCodeLong = 0x81, // 64-bit two's-complement integer in network byte order (8)
        TypeCodeSmalllong = 0x55, // 8-bit two's-complement integer

        // Decimal
        TypeCodeFloat = 0x72, // IEEE 754-2008 binary32 (4)
        TypeCodeDouble = 0x82, // IEEE 754-2008 binary64 (8)
        TypeCodeDecimal32 = 0x74, // IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding (4)
        TypeCodeDecimal64 = 0x84, // IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding (8)
        TypeCodeDecimal128 = 0x94, // IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding (16)


        TypeCodeVbin8 = 0xa0, // up to 2^8 - 1 octets of binary data (1 + variable)
        TypeCodeVbin32 = 0xb0, // up to 2^32 - 1 octets of binary data (4 + variable)
        TypeCodeStr8 = 0xa1, // up to 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (1 + variable)
        TypeCodeStr32 = 0xb1, // up to 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark) (4 +variable)
        TypeCodeSym8 = 0xa3, // up to 2^8 - 1 seven bit ASCII characters representing a symbolic value (1 + variable)
        TypeCodeSym32 = 0xb3, // up to 2^32 - 1 seven bit ASCII characters representing a symbolic value (4 + variable)


        // Compound
        TypeCodeList0 = 0x45, // the empty list (i.e. the list with no elements) (0)
        TypeCodeList8 = 0xc0, // up to 2^8 - 1 list elements with total size less than 2^8 octets (1 + compound)
        TypeCodeList32 = 0xd0, // up to 2^32 - 1 list elements with total size less than 2^32 octets (4 + compound)
        TypeCodeMap8 = 0xc1, // up to 2^8 - 1 octets of encoded map data (1 + compound)
        TypeCodeMap32 = 0xd1, // up to 2^32 - 1 octets of encoded map data (4 + compound)
        TypeCodeArray8 = 0xe0, // up to 2^8 - 1 array elements with total size less than 2^8 octets (1 + array)
        TypeCodeArray32 = 0xf0, // up to 2^32 - 1 array elements with total size less than 2^32 octets (4 + array)
    }

    public enum FrameType
    {
        TypeCodeApplicationData = 0x75,
        TypeCodeMessageAnnotations = 0x72
    }
}