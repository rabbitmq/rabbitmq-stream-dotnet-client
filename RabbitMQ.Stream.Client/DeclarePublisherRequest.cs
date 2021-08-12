using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeclarePublisherRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly byte publisherId;
        private readonly string publisherRef;
        private readonly string stream;
        public const ushort Key = 1;

        public DeclarePublisherRequest(uint correlationId, byte publisherId, string publisherRef, string stream)
        {
            this.correlationId = correlationId;
            this.publisherId = publisherId;
            this.publisherRef = publisherRef;
            this.stream = stream;
        }

        public int SizeNeeded => 8 + 1  + WireFormatting.StringSize(publisherRef) + WireFormatting.StringSize(stream);

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteByte(span.Slice(offset), publisherId);
            offset += WireFormatting.WriteString(span.Slice(offset), publisherRef);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            return offset;
        }

        public void Dispose()
        {
        }
    }
}
