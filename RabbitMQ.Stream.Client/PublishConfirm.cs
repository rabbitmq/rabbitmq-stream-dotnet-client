using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishConfirm : ICommand
    {
        public const ushort Key = 3;
        private readonly byte publisherId;
        private readonly ulong[] publishingIds;

        private PublishConfirm(byte publisherId, ulong[] publishingIds)
        {
            this.publisherId = publisherId;
            this.publishingIds = publishingIds;
        }

        public byte PublisherId => publisherId;

        public ulong[] PublishingIds => publishingIds;

        public int SizeNeeded => throw new NotImplementedException();

        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            var offset = 2; //WireFormatting.ReadUInt16(frame, out var tag);
            offset += 2; //WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadByte(frame.Slice(offset), out var publisherId);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out var numIds);
            var publishingIds = new ulong[numIds];
            for (var i = 0; i < numIds; i++)
            {
                offset += WireFormatting.ReadUInt64(frame.Slice(offset), out publishingIds[i]);
            }
            command = new PublishConfirm(publisherId, publishingIds);
            
            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
