// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CreateRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly string stream;
        private readonly IDictionary<string, string> arguments;
        public const ushort Key = 13;

        public CreateRequest(uint correlationId, string stream, IDictionary<string, string> arguments)
        {
            this.correlationId = correlationId;
            this.stream = stream;
            this.arguments = arguments;
        }
        public int SizeNeeded
        {
            get
            {
                var argSize = 0;
                foreach (var (key, value) in arguments)
                {
                    argSize += WireFormatting.StringSize(key) + WireFormatting.StringSize(value);
                }

                return 8 + WireFormatting.StringSize(stream) + 4 + argSize;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            offset += WireFormatting.WriteInt32(span.Slice(offset), arguments.Count);

            foreach (var (key, value) in arguments)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), key);
                offset += WireFormatting.WriteString(span.Slice(offset), value);
            }

            return offset;
        }
    }

    public readonly struct CreateResponse : ICommand
    {
        public const ushort Key = 13;
        private readonly uint correlationId;
        private readonly ushort responseCode;

        public CreateResponse(uint correlationId, ushort responseCode)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => (ResponseCode)responseCode;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
        internal static int Read(ReadOnlySequence<byte> frame, out CreateResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            command = new CreateResponse(correlation, responseCode);
            return offset;
        }
    }
}
