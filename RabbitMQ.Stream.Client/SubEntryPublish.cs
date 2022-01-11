using System;
using System.Collections.Generic;
using System.Linq;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SubEntryPublish : ICommand
    {
        private const ushort Key = 2;
        public byte Version => 1;

        public int SizeNeeded
        {
            get
            {
//                msgLen += ((8 + 1 + 2 + 4 + 4) * len(aggregation.items)) + aggregation.totalSizeInBytes
                var initBufferPublishSize = 2 + 2 + 1 + 4;
                var len = initBufferPublishSize + (8 + 1 + 2 + 4 + 4) +
                          messages.Sum(msg => 4 + msg.Size);
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
            
            offset += WireFormatting.WriteUInt64(span.Slice(offset), publishingId);
            var agg = (byte) compressMode << 4;
            offset += WireFormatting.WriteByte(
                span.Slice(offset), (byte) (0x80 | agg));
            offset += WireFormatting.WriteUInt16(span.Slice(offset), (ushort)MessageCount);
            // compressed size
            offset += WireFormatting.WriteUInt32(span.Slice(offset),
                (uint) messages.Sum(msg => 4 + msg.Size));

            // uncompressed size
            offset += WireFormatting.WriteUInt32(span.Slice(offset),
                (uint) messages.Sum(msg => 4 + msg.Size));


            foreach (var msg in messages)
            {
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            return offset;
        }

        private readonly byte publisherId;
        private readonly List<Message> messages;
        private readonly ulong publishingId;
        private readonly CompressMode compressMode;
        public int MessageCount { get; }

        public SubEntryPublish(byte publisherId, ulong publishingId,
            List<Message> messages, CompressMode compressMode)
        {
            this.publisherId = publisherId;
            this.publishingId = publishingId;
            this.messages = messages;
            this.MessageCount = messages.Count;
            this.compressMode = compressMode;
        }
    }
}