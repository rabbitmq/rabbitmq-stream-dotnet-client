using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryPublisherRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string publisherRef;
        private readonly string stream;
        public const ushort Key = 5;

        public QueryPublisherRequest(uint correlationId, string publisherRef, string stream)
        {
            this.correlationId = correlationId;
            this.publisherRef = publisherRef;
            this.stream = stream;
        }
        public int SizeNeeded => 8 + WireFormatting.StringSize(publisherRef) + WireFormatting.StringSize(stream);

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), publisherRef);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            return offset;
        }
    }
}
