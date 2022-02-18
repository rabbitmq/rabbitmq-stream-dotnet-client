// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

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

        internal static int Read(ReadOnlySequence<byte> frame, out SaslHandshakeResponse command)
        {
            uint correlation;
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            uint numMechs;
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out numMechs);
            var mechs = new List<string>();
            for (var i = 0; i < numMechs; i++)
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
