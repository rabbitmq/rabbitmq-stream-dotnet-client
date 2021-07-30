using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public interface IOffsetType
    {
        int Size { get; }

        int Write(Span<byte> span);
    }
    public readonly struct OffsetTypeFirst : IOffsetType
    {
        public int Size => 2;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, 1);
            return 2;
        }
    }
    public readonly struct OffsetTypeLast : IOffsetType
    {
        public int Size => 2;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, 2);
            return 2;
        }
    }
    public readonly struct OffsetTypeNext : IOffsetType
    {
        public int Size => 2;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, 3);
            return 2;
        }
    }

    public readonly struct OffsetTypeOffset : IOffsetType
    {
        private readonly ulong offset;

        public OffsetTypeOffset(ulong offset)
        {
            this.offset = offset;
        }

        public int Size => 10;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, 4);
            WireFormatting.WriteUInt64(span, offset);
            return 10;
        }
    }

    public readonly struct OffsetTypeTimestamp : IOffsetType
    {
        private readonly long timestamp;

        public OffsetTypeTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
        }

        public int Size => 10;

        public int Write(Span<byte> span)
        {
            WireFormatting.WriteUInt16(span, 5);
            WireFormatting.WriteInt64(span, timestamp);
            return 10;
        }
    }
    public readonly struct SubscribeResponse : ICommand
    {
        public const ushort Key = 7;
        private readonly uint correlationId;
        private readonly ushort responseCode;

        public SubscribeResponse(uint correlationId, ushort responseCode)
        {
            this.correlationId = correlationId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public uint CorrelationId => correlationId;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();

        }
        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort tag;
            ushort version;
            uint correlation;
            ushort responseCode;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out responseCode);
            command = new SubscribeResponse(correlation, responseCode);
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

        public SubscribeRequest(uint correlationId, byte subscriptionId, string stream, IOffsetType offsetType, ushort credit, IDictionary<string, string> properties)
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
                int size = 9 + 2 + WireFormatting.StringSize(stream) + offsetType.Size + 2 + 4;
                foreach (var (k, v) in properties)
                {
                    // TODO: unnecessary conversion work here to work out the correct size of the frame
                    size += WireFormatting.StringSize(k) + WireFormatting.StringSize(v) + 4; //
                }

                return size;
            }
        }

        public int Write(Span<byte> span)
        {
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), ((ICommand) this).Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            offset += WireFormatting.WriteByte(span.Slice(offset), subscriptionId);
            offset += WireFormatting.WriteString(span.Slice(offset), stream);
            offset += offsetType.Write(span.Slice(offset));
            offset += WireFormatting.WriteUInt16(span.Slice(offset), credit);
            offset += WireFormatting.WriteInt32(span.Slice(offset), properties.Count);
            foreach(var (k,v) in properties)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), k);
                offset += WireFormatting.WriteString(span.Slice(offset), v);
            }

            return offset;
        }

        public void Dispose()
        {
        }
    }
}
