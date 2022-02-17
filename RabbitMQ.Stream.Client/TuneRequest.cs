// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

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
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), frameMax);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), heartbeat);
            return offset;
        }

        public static void Dispose()
        {
        }
    }
}
