using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryOffsetResponse : ICommand
    {
        public const ushort Key = 11;
        private readonly uint correlationId;
        private readonly ResponseCode responseCode;
        private readonly ulong offsetValue;

        public QueryOffsetResponse(uint correlationId, ResponseCode responseCode, ulong offsetValue)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
            this.offsetValue = offsetValue;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => responseCode;
        public ulong Offset => offsetValue;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out QueryOffsetResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var key);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadUInt64(frame.Slice(offset), out var offsetValue);
            command = new QueryOffsetResponse(correlation, (ResponseCode) responseCode, offsetValue);
            return offset;
        }
    }
}