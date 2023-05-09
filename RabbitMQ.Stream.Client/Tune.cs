// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct TuneResponse : ICommand
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

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
            offset += WireFormatting.WriteUInt32(span[offset..], frameMax);
            offset += WireFormatting.WriteUInt32(span[offset..], heartbeat);
            writer.Advance(offset);
            return offset;
        }

        internal static int Read(ReadOnlySequence<byte> frame, out TuneResponse command)
        {
            uint frameMax;
            uint heartbeat;
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out frameMax);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out heartbeat);
            command = new TuneResponse(frameMax, heartbeat);
            return offset;
        }
    }
}
