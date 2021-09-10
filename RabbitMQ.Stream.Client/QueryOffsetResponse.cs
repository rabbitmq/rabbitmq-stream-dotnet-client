using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryOffsetResponse : ICommand
    {
        public const ushort Key = 11;
        private readonly uint correlationId;
        private readonly ushort responseCode;
        private readonly uint offsetValue;

        public QueryOffsetResponse(uint correlationId, ushort responseCode, uint offsetValue)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
            this.offsetValue = offsetValue;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ushort ResponseCode => responseCode;
        public ulong Offset => offsetValue;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort key;
            ushort version;
            uint correlation;
            ushort responseCode;
            ulong offsetValue;
            var offset = WireFormatting.ReadUInt16(frame, out key);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out responseCode);
            offset += WireFormatting.ReadUInt64(frame.Slice(offset), out offsetValue);
            command = new CloseResponse(correlation, responseCode);
            return offset;
        }
    }
}