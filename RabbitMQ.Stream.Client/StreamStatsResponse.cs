// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct StreamStatsResponse : ICommand
    {
        public const ushort Key = 0x001c;
        private readonly uint correlationId;
        private readonly ResponseCode responseCode;
        public IDictionary<string, long> Statistic { get; }

        public StreamStatsResponse(uint correlationId, ResponseCode responseCode, IDictionary<string, long> statistic)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
            Statistic = statistic;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => responseCode;

        public int Write(IBufferWriter<byte> writer)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out StreamStatsResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out var numProps);
            var props = new Dictionary<string, long>();
            for (var i = 0; i < numProps; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out var key);
                offset += WireFormatting.ReadInt64(frame.Slice(offset), out var value);
                props.Add(key, value);
            }

            command = new StreamStatsResponse(correlation, (ResponseCode)responseCode, props);
            return offset;
        }
    }
}
