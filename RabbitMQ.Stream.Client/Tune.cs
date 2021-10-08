using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct TuneResponse : ICommand
    {
        public const ushort Key = 20;
        private readonly uint frameMax;
        private readonly uint heartbeat;

        public TuneResponse(uint frameMax, uint heartbeat)
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

        internal static int Read(ReadOnlySequence<byte> frame, out TuneResponse command)
        {
            ushort tag;
            ushort version;
            uint frameMax;
            uint heartbeat;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out frameMax);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out heartbeat);
            command = new TuneResponse(frameMax, heartbeat);
            return offset;
        }
    }
}