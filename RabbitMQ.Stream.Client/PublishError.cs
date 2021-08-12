using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishError : ICommand
    {
        public const ushort Key = 4;
        private readonly byte publisherId;
        private readonly (ulong, ushort)[] publishingErrors;

        public PublishError(byte publisherId, (ulong, ushort)[] publishingErrors)
        {
            this.publisherId = publisherId;
            this.publishingErrors = publishingErrors;
        }

        public byte PublisherId => publisherId;

        public (ulong, ushort)[] PublishingErrors => publishingErrors;

        public int SizeNeeded => throw new NotImplementedException();

        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort tag;
            ushort version;
            byte publisherId;
            int numErrors;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadByte(frame.Slice(offset), out publisherId);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out numErrors);
            var publishingIds = new (ulong, ushort)[numErrors];
            for (int i = 0; i < numErrors; i++)
            {
                ulong pubId;
                ushort code;
                offset += WireFormatting.ReadUInt64(frame.Slice(offset), out pubId);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out code);
                publishingIds[i] = (pubId, code);
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
