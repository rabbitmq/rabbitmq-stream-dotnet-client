﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public enum OffsetTypeEnum
    {
        First = 1,
        Last = 2,
        Next = 3,
        Offset = 4,
        Timestamp = 5
    }

    public interface IOffsetType
    {
        int Size { get; }

        OffsetTypeEnum OffsetType { get; }

        int Write(Span<byte> span);
    }

    public readonly struct OffsetTypeFirst : IOffsetType
    {
        public int Size => 2;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.First;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            return 2;
        }
    }

    public readonly struct OffsetTypeLast : IOffsetType
    {
        public int Size => 2;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Last;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            return 2;
        }
    }

    public readonly struct OffsetTypeNext : IOffsetType
    {
        public int Size => 2;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Next;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            return 2;
        }
    }

    public readonly struct OffsetTypeOffset : IOffsetType
    {
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Offset;

        public OffsetTypeOffset(ulong offset)
        {
            OffsetValue = offset;
        }

        internal ulong OffsetValue { get; }

        public int Size => 2 + 8;

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            offset += WireFormatting.WriteUInt64(span[offset..], OffsetValue);
            return offset;
        }
    }

    public readonly struct OffsetTypeTimestamp : IOffsetType
    {
        private readonly long timestamp;
        public OffsetTypeEnum OffsetType => OffsetTypeEnum.Timestamp;

        public OffsetTypeTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
        }

        internal long TimeStamp => timestamp;
        public int Size => 10;

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, (ushort)OffsetType);
            offset += WireFormatting.WriteInt64(span[offset..], timestamp);
            return offset;
        }
    }

    public readonly struct SubscribeResponse : ICommand
    {
        public const ushort Key = 7;
        private readonly uint correlationId;
        private readonly ResponseCode responseCode;

        private SubscribeResponse(uint correlationId, ResponseCode responseCode)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public ResponseCode ResponseCode => responseCode;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out SubscribeResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            command = new SubscribeResponse(correlation, (ResponseCode)responseCode);
            return offset;
        }
    }

    public readonly struct SubscribeRequest : ICommand
    {
        private readonly uint correlationId;
        private readonly byte subscriptionId;
        private readonly string stream;
        private readonly IOffsetType offsetType;
        private readonly ushort credit;
        private readonly IDictionary<string, string> properties;
        public const ushort Key = 7;

        public SubscribeRequest(uint correlationId, byte subscriptionId, string stream, IOffsetType offsetType,
            ushort credit, IDictionary<string, string> properties)
        {
            this.correlationId = correlationId;
            this.subscriptionId = subscriptionId;
            this.stream = stream;
            this.offsetType = offsetType;
            this.credit = credit;
            this.properties = properties;
        }

        public int SizeNeeded
        {
            get
            {
                var size = 2 + 2 + 4 + 1 + WireFormatting.StringSize(stream) + offsetType.Size + 2;
                if (properties.Count > 0)
                {
                    size += 4; // size of the properties map
                }

                foreach (var (key, value) in properties)
                {
                    // TODO: unnecessary conversion work here to work out the correct size of the frame
                    size += WireFormatting.StringSize(key) + WireFormatting.StringSize(value); //
                }

                return size;
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand)this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteByte(span.Slice(offset), subscriptionId);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            offset += offsetType.Write(span.Slice(offset));
            offset += WireFormatting.WriteUInt16(span.Slice(offset), credit);
            if (properties.Count > 0)
            {
                offset += WireFormatting.WriteInt32(span.Slice(offset), properties.Count);
                foreach (var (k, v) in properties)
                {
                    offset += WireFormatting.WriteString(span.Slice(offset), k);
                    offset += WireFormatting.WriteString(span.Slice(offset), v);
                }
            }

            return offset;
        }
    }
}
