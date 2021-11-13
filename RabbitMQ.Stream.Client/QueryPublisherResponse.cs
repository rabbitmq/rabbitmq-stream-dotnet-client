using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryPublisherResponse : ICommand
    {
        private readonly uint correlationId;
        private readonly ResponseCode responseCode;
        private readonly ulong sequence ;
        public const ushort Key = 5;

        public QueryPublisherResponse(uint correlationId, ResponseCode responseCode, ulong sequence)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
            this.sequence = sequence;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => responseCode;

        public ulong Sequence => sequence;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
        internal static int Read(ReadOnlySequence<byte> frame, out QueryPublisherResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadUInt64(frame.Slice(offset), out var sequence);
            command = new QueryPublisherResponse(correlation, (ResponseCode)responseCode, sequence);
            return offset;
        }
    }
}
