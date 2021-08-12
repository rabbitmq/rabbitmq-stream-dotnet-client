using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct TuneRequest : ICommand
    {
        public const ushort Key = 20;
        private readonly uint frameMax;
        private readonly uint heartbeat;

        public TuneRequest(uint frameMax, uint heartbeat)
        {
            this.frameMax = frameMax;
            this.heartbeat = heartbeat;
        }
        public int SizeNeeded => 12;

        public uint FrameMax => frameMax;

        public uint Heartbeat => heartbeat;

        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), frameMax);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), heartbeat);
            return offset;
        }

        public void Dispose()
        {
        }
    }
}