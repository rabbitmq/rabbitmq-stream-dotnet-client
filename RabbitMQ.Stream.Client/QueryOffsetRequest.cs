using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryOffsetRequest : ICommand
    {
        public const ushort Key = 11;
        private readonly string stream;
        private readonly uint correlationId;
        private readonly string reference;

        public QueryOffsetRequest(string stream, uint correlationId, string reference)
        {
            this.stream = stream;
            this.correlationId = correlationId;
            this.reference = reference;
        }


        public uint CorrelationId => correlationId;

        public int SizeNeeded =>
            2 + 2 + 4 +  WireFormatting.StringSize(reference) + WireFormatting.StringSize(stream);


        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), reference);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            return offset;
        }

        public void Dispose()
        {
        }
    }
}