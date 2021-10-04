using System;
using System.Buffers;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslAuthenticateRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string mechanism;
        private readonly byte[] data;
        public const ushort Key = 19;

        public SaslAuthenticateRequest(uint correlationId, string mechanism, byte[] data)
        {
            this.correlationId = correlationId;
            this.mechanism = mechanism;
            this.data = data;
        }

        public int SizeNeeded => 4 + 4 + WireFormatting.StringSize(mechanism) + 4 + data.Length;

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), mechanism);
            offset += WireFormatting.WriteBytes(span.Slice(offset), new ReadOnlySequence<byte>(data));
            return offset;
        }

        public void Dispose()
        {
        }
    }
}
