using System;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CloseRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string reason;
        public const ushort Key = 22;

        public CloseRequest(uint correlationId, string reason)
        {
            this.correlationId = correlationId;
            this.reason = reason;
        }
        public int SizeNeeded => 10 + WireFormatting.StringSize(reason);

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), 1); //ok code
            offset += WireFormatting.WriteString(span.Slice(offset), reason);
            return offset;
        }
    }
}
