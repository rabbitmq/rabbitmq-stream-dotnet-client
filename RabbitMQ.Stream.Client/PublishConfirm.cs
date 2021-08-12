using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishConfirm : ICommand
    {
        public const ushort Key = 3;
        private readonly byte publisherId;
        private readonly ulong[] publishingIds;

        public PublishConfirm(byte publisherId, ulong[] publisingIds)
        {
            this.publisherId = publisherId;
            this.publishingIds = publisingIds;
        }

        public byte PublisherId => publisherId;

        public ulong[] PublishingIds => publishingIds;

        public int SizeNeeded => throw new NotImplementedException();

        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort tag;
            ushort version;
            byte publisherId;
            int numIds;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadByte(frame.Slice(offset), out publisherId);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out numIds);
            ulong[] publishingIds = new ulong[numIds];
            for (int i = 0; i < numIds; i++)
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
