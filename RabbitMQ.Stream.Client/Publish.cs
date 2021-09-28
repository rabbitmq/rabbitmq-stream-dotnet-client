using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Publish : ICommand
    {
        private const ushort Key = 2;

        public int SizeNeeded
        {
            get
            {
                var size = 9; // pre amble 
                foreach (var (_, msg) in messages)
                {
                    size += 8 + 4 + msg.Size;
                }
                
                return size;
             }
        }

        private readonly byte publisherId;
        private readonly IList<(ulong, Message)> messages;

        public Publish(byte publisherId, IList<(ulong, Message)> messages)
        {
            this.publisherId = publisherId;
            this.messages = messages;
        }

        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteByte(span.Slice(offset), publisherId);
            // this assumes we never write an empty publish frame
            offset += WireFormatting.WriteInt32(span.Slice(offset), messages.Count);
            foreach(var (publishingId, msg) in messages)
            {
                offset += WireFormatting.WriteUInt64(span.Slice(offset), publishingId);
                // this only write "simple" messages, we assume msg is just the binary body
                // not stream encoded data
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            return offset;
        }
    }
}
