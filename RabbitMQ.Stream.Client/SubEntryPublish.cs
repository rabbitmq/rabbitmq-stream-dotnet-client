// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SubEntryPublish : ICommand
    {
        private const ushort Key = 2;
        public static byte Version => 1;

        public int SizeNeeded
        {
            get
            {
                const int headerSize = 2 + 2 + 1 + 4;
                var len = headerSize + 8 + 1 + 2 + 4 + 4 + compressionCodec.CompressedSize;
                return len;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), Version);
            offset += WireFormatting.WriteByte(span.Slice(offset), publisherId);
            // number of root messages. In this case will be always 1. 
            offset += WireFormatting.WriteInt32(span.Slice(offset), 1);
            // publishingId for all the messages
            // so there is publishingId --> []messages
            offset += WireFormatting.WriteUInt64(span.Slice(offset), publishingId);
            // compress mode see CompressMode
            var agg = (byte)compressionCodec.CompressionType << 4;
            offset += WireFormatting.WriteByte(
                span.Slice(offset), (byte)(0x80 | agg));

            // sub Messages number  
            offset += WireFormatting.WriteUInt16(span.Slice(offset), (ushort)compressionCodec.MessagesCount);

            // uncompressed byte size value
            offset += WireFormatting.WriteUInt32(span.Slice(offset),
                (uint)compressionCodec.UnCompressedSize);

            // compressed byte size value 
            offset += WireFormatting.WriteUInt32(span.Slice(offset),
                (uint)compressionCodec.CompressedSize);

            offset += compressionCodec.Write(span.Slice(offset));
            return offset;
        }

        private readonly byte publisherId;
        private readonly ulong publishingId;
        private readonly ICompressionCodec compressionCodec;

        public SubEntryPublish(byte publisherId, ulong publishingId, ICompressionCodec compressionCodec)
        {
            this.publisherId = publisherId;
            this.publishingId = publishingId;
            this.compressionCodec = compressionCodec;
        }
    }
}
