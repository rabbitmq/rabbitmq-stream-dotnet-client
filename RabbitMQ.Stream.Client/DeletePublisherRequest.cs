using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeletePublisherRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly byte publisherId;
        public const ushort Key = 6;

        public DeletePublisherRequest(uint correlationId, byte publisherId)
        {
            this.correlationId = correlationId;
            this.publisherId = publisherId;
        }

        public int SizeNeeded => 8 + 1;

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteByte(span.Slice(offset), publisherId);
            return offset;
        }
    }
}
