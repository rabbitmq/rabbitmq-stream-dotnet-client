// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishError : ICommand
    {
        public const ushort Key = 4;
        private readonly byte publisherId;
        private readonly (ulong, ResponseCode)[] publishingErrors;

        private PublishError(byte publisherId, (ulong, ResponseCode)[] publishingErrors)
        {
            this.publisherId = publisherId;
            this.publishingErrors = publishingErrors;
        }

        public byte PublisherId => publisherId;

        public (ulong, ResponseCode)[] PublishingErrors => publishingErrors;

        public int SizeNeeded => throw new NotImplementedException();

        internal static int Read(ReadOnlySequence<byte> frame, out PublishError command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadByte(frame.Slice(offset), out var publisherId);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out var numErrors);
            var publishingIds = new (ulong, ResponseCode)[numErrors];
            for (var i = 0; i < numErrors; i++)
            {
                offset += WireFormatting.ReadUInt64(frame.Slice(offset), out var pubId);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var code);
                publishingIds[i] = (pubId, (ResponseCode)code);
            }

            command = new PublishError(publisherId, publishingIds);
            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
