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
                // SubBatch publish protocol is different from the standard 
                // publish
                const int headerSize = 2 + 2 + 1 + 4;
                var len = headerSize + (8 + 1 + 2 + 4 + 4) +
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
            // publishingId for all the messages
            // so there is publishingId --> []messages
            offset += WireFormatting.WriteUInt64(span.Slice(offset), publishingId);
            // compress mode see CompressMode
            var agg = (byte) compressMode << 4;
            offset += WireFormatting.WriteByte(
                span.Slice(offset), (byte) (0x80 | agg));
            
            // sub Messages number  
            offset += WireFormatting.WriteUInt16(span.Slice(offset), (ushort)MessageCount);
            
            // compressed byte size value 
            offset += WireFormatting.WriteUInt32(span.Slice(offset),
                (uint) messages.Sum(msg => 4 + msg.Size));

            // uncompressed byte size value
            offset += WireFormatting.WriteUInt32(span.Slice(offset),
                (uint) messages.Sum(msg => 4 + msg.Size));

            
            // message with size and value
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