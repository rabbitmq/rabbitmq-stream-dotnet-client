using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct QueryPublisherResponse : ICommand
    {
        private readonly uint correlationId;
        private readonly ushort responseCode;
        private readonly ulong sequence ;
        public const ushort Key = 5;

        public QueryPublisherResponse(uint correlationId, ushort responseCode, ulong sequence)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
            this.sequence = sequence;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ushort ResponseCode => responseCode;

        public ulong Sequence => sequence;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort tag;
            ushort version;
            uint correlation;
            ushort responseCode;
            ulong sequence;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out responseCode);
            offset += WireFormatting.ReadUInt64(frame.Slice(offset), out sequence);
            command = new QueryPublisherResponse(correlation, responseCode, sequence);
            return offset;
        }
    }
}
