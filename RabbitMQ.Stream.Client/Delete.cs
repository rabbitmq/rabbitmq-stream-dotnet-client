using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;

namespace RabbitMQ.Stream.Client
{
    public readonly struct DeleteRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string stream;
        public const ushort Key = 14;

        public DeleteRequest (uint correlationId, string stream)
        {
            this.correlationId = correlationId;
            this.stream = stream;
        }
        
        public int SizeNeeded => 8 + WireFormatting.StringSize(stream);

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            return offset;
        }
    }
    
    public readonly struct DeleteResponse : ICommand
    {
        public const ushort Key = 14;
        private readonly uint correlationId;
        private readonly ushort responseCode;
        
        public DeleteResponse(uint correlationId, ushort responseCode)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => (ResponseCode)responseCode;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            command = new DeleteResponse(correlation, responseCode);
            return offset;
        }
    }
}
