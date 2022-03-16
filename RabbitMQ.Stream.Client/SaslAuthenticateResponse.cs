﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct SaslAuthenticateResponse : ICommand
    {
        private readonly uint correlationId;
        private readonly ResponseCode responseCode;
        private readonly byte[] data;
        public const ushort Key = 19;

        public SaslAuthenticateResponse(uint correlationId, ResponseCode code, byte[] data)
        {
            this.correlationId = correlationId;
            responseCode = code;
            this.data = data;
        }

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => responseCode;

        public byte[] Data => data;

        public int SizeNeeded => throw new NotImplementedException();

        internal static int Read(ReadOnlySequence<byte> frame, out SaslAuthenticateResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            var data = Array.Empty<byte>();
            if (frame.Length > offset)
            {
                offset += WireFormatting.ReadBytes(frame.Slice(offset), out data);
            }

            command = new SaslAuthenticateResponse(correlation, (ResponseCode)responseCode, data);

            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
