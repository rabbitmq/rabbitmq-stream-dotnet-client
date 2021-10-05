using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct OpenResponse : ICommand
    {
        private readonly uint correlationId;
        private readonly ushort responseCode;
        private readonly IDictionary<string, string> connectionProperties;
        public const ushort Key = 21;

        private OpenResponse(uint correlationId, ushort responseCode, IDictionary<string, string> connectionProperties)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
            this.connectionProperties = connectionProperties;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ushort ResponseCode => responseCode;

        public IDictionary<string, string> ConnectionProperties => connectionProperties;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
        internal static int Read(ReadOnlySequence<byte> frame, out OpenResponse command)
        {
            ushort tag;
            ushort version;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out var numProps);
            var props = new Dictionary<string, string>();
            for (var i = 0; i < numProps; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out var k);
                offset += WireFormatting.ReadString(frame.Slice(offset), out var v);
                props.Add(k, v);
            }
            command = new OpenResponse(correlation, responseCode, props);
            return offset;
        }
    }
}
