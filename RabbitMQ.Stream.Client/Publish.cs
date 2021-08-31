using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Publish : ICommand
    {
        public const ushort Key = 2;

        public int SizeNeeded
        {
            get
            {
                var size = 9; // pre amble 
                foreach (var (_, msg) in messages)
                {
                    size += 8 + 4 + (int)msg.Length;
                }
                return size;
             }
        }

        private readonly byte publisherId;
        private readonly IList<(ulong, ReadOnlySequence<byte>)> messages;

        public Publish(byte publisherId, IList<(ulong, ReadOnlySequence<byte>)> messages)
        {
            this.publisherId = publisherId;
            this.messages = messages;
        }

        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteByte(span.Slice(offset), publisherId);
            // this assumes we never write an empty publish frame
            offset += WireFormatting.WriteInt32(span.Slice(offset), messages.Count);
            foreach(var (publisherId, msg) in messages)
            {
                offset += WireFormatting.WriteUInt64(span.Slice(offset), publisherId);
                // this only write "simple" messages, we assume msg is just the binary body
                // not stream encoded data
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Length);
                offset += WireFormatting.Write(span.Slice(offset), msg);
            }

            return offset;
        }
    }
}
