﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    internal readonly struct TuneRequest : ICommand
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

        public int Write(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(SizeNeeded);
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span[offset..], frameMax);
            offset += WireFormatting.WriteUInt32(span[offset..], heartbeat);
            writer.Advance(offset);
            return offset;
        }
    }
}
