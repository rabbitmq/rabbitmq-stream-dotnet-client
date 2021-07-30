using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslAuthenticateResponse : ICommand
    {
        private readonly uint correlationId;
        private readonly ushort responseCode;
        private readonly byte[] data;
        public const ushort Key = 19;

        public SaslAuthenticateResponse(uint correlationId, ushort code, byte[] data)
        {
            this.correlationId = correlationId;
            this.responseCode = code;
            this.data = data;
        }

        public uint CorrelationId => correlationId;

        public ushort ResponseCode => responseCode;

        public byte[] Data => data;

        public int SizeNeeded => throw new NotImplementedException();

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
            byte[] data = new byte[0];
            if(frame.Length > offset)
            {
                offset += WireFormatting.ReadBytes(frame.Slice(offset), out data);
            }

            command = new SaslAuthenticateResponse(correlation, responseCode, data);

            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
