using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslHandshakeResponse : ICommand
    {
        private readonly uint correlationId;
        private readonly IList<string> mechanisms;
        public const ushort Key = 18;

        public SaslHandshakeResponse(uint correlationId, IEnumerable<string> mechanisms)
        {
            this.correlationId = correlationId;
            this.mechanisms = mechanisms.ToList();
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public IList<string> Mechanisms => mechanisms;

        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort tag;
            ushort version;
            uint correlation;
            ushort responseCode;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out responseCode);
            uint numMechs;
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out numMechs);
            var mechs = new List<string>();
            for (int i = 0; i < numMechs; i++)
            {
                string mech;
                offset += WireFormatting.ReadString(frame.Slice(offset), out mech);
                mechs.Add(mech);
            }
            // Console.WriteLine($"reading sasl response {responseCode} {mech}");
            command = new SaslHandshakeResponse(correlation, mechs);
            return offset;
        }
        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
