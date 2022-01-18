using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    //Deliver => Key Version SubscriptionId OsirisChunk
    //   Key => uint16 // 8
    //   Version => uint32
    //   SubscriptionId => uint8
    public readonly struct Deliver : ICommand
    {
        private readonly byte subscriptionId;
        private readonly Chunk chunk;
        public const ushort Key = 8;
        public int SizeNeeded => throw new NotImplementedException();

        private Deliver(byte subscriptionId, Chunk chunk)
        {
            this.subscriptionId = subscriptionId;
            this.chunk = chunk;
        }

        public IEnumerable<MsgEntry> Messages
        {
            get
            {
                var offset = 0;
                if (chunk.IsSubBatch)
                {
                    var data = chunk.Data;
                    var numRecords = chunk.NumRecords;

                    while (numRecords != 0)
                    {
                        SubBatchChunk.Read(data.Slice(offset), out var subBatchChunk);
                        var unCompressedData =
                            CompressHelper.UnCompress(subBatchChunk.Data,
                                subBatchChunk.DataSize,
                                subBatchChunk.UnCompressedDataSize,
                                subBatchChunk.CompressMode);
                        for (ulong z = 0; z < subBatchChunk.NumRecordsInBatch; z++)
                        {
                            offset +=
                                WireFormatting.ReadUInt32(unCompressedData.Slice(offset),
                                    out var len);
                            var entry = new MsgEntry(chunk.ChunkId + z, chunk.Epoch,
                                unCompressedData.Slice(offset, len));
                            offset += (int) len;
                            yield return entry;
                        }

                        numRecords -= subBatchChunk.NumRecordsInBatch;
                    }
                }
                else
                {
                    var data = chunk.Data;
                    for (ulong i = 0; i < chunk.NumEntries; i++)
                    {
                        offset += WireFormatting.ReadUInt32(data.Slice(offset), out var len);
                        //TODO: assuming only simple entries for now
                        var entry = new MsgEntry(chunk.ChunkId + i, chunk.Epoch, data.Slice(offset, len));
                        offset += (int) len;
                        yield return entry;
                    }
                }
            }
        }

        public Chunk Chunk => chunk;

        public byte SubscriptionId => subscriptionId;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out Deliver command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadByte(frame.Slice(offset), out var subscriptionId);
            offset += Chunk.Read(frame.Slice(offset), out var chunk);
            command = new Deliver(subscriptionId, chunk);
            return offset;
        }
    }

    internal readonly struct SubBatchChunk
    {
        private readonly byte compressValue;

        public SubBatchChunk(byte compressionValue, ushort numRecordsInBatch, uint unCompressedDataSize, uint dataSize,
            ReadOnlySequence<byte> data)
        {
            compressValue = compressionValue;
            NumRecordsInBatch = numRecordsInBatch;
            UnCompressedDataSize = unCompressedDataSize;
            DataSize = dataSize;
            Data = data;
        }

        public CompressMode CompressMode => (CompressMode) compressValue;

        public ushort NumRecordsInBatch { get; }

        public uint UnCompressedDataSize { get; }

        public uint DataSize { get; }

        public ReadOnlySequence<byte> Data { get; }


        internal static int Read(ReadOnlySequence<byte> seq, out SubBatchChunk subBatchChunk)
        {
            var compressionValue = (byte) 0;
            var offset = 0;
            offset = WireFormatting.ReadByte(seq.Slice(offset), out var compression);
            offset += WireFormatting.ReadUInt16(seq.Slice(offset), out var numRecordsInBatch);
            offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var unCompressedDataSize);
            offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var dataSize);
            compressionValue = (byte) ((byte) (compression & 0x70) >> 4);
            var data = seq.Slice(offset, dataSize);
            subBatchChunk =
                new SubBatchChunk(compressionValue, numRecordsInBatch, unCompressedDataSize, dataSize, data);
            return offset;
        }
    }


    public readonly struct MsgEntry
    {
        private readonly ulong offset;
        private readonly ulong epoch;
        private readonly ReadOnlySequence<byte> data;

        public MsgEntry(ulong offset, ulong epoch, ReadOnlySequence<byte> data)
        {
            this.offset = offset;
            this.epoch = epoch;
            this.data = data;
        }

        public ulong Offset => offset;

        public ulong Epoch => epoch;

        public ReadOnlySequence<byte> Data => data;
    }

    public readonly struct Chunk
    {
        private Chunk(byte magicVersion,
            ushort numEntries,
            uint numRecords,
            long timestamp,
            ulong epoch,
            ulong chunkId,
            int crc,
            ReadOnlySequence<byte> data, bool isSubBatch)
        {
            MagicVersion = magicVersion;
            NumEntries = numEntries;
            NumRecords = numRecords;
            Timestamp = timestamp;
            Epoch = epoch;
            ChunkId = chunkId;
            Crc = crc;
            IsSubBatch = isSubBatch;
            Data = data;
        }

        public bool IsSubBatch { get; }


        public byte MagicVersion { get; }

        public ushort NumEntries { get; }
        public uint NumRecords { get; }
        public long Timestamp { get; }
        public ulong Epoch { get; }
        public ulong ChunkId { get; }
        public int Crc { get; }
        public ReadOnlySequence<byte> Data { get; }

        internal static int Read(ReadOnlySequence<byte> seq, out Chunk chunk)
        {
            var offset = WireFormatting.ReadByte(seq, out var magicVersion);
            offset += WireFormatting.ReadByte(seq.Slice(offset), out var chunkType);
            offset += WireFormatting.ReadUInt16(seq.Slice(offset), out var numEntries);
            offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var numRecords);
            offset += WireFormatting.ReadInt64(seq.Slice(offset), out var timestamp);
            offset += WireFormatting.ReadUInt64(seq.Slice(offset), out var epoch);
            offset += WireFormatting.ReadUInt64(seq.Slice(offset), out var chunkId);
            offset += WireFormatting.ReadInt32(seq.Slice(offset), out var crc);
            offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var dataLen);
            offset += WireFormatting.ReadUInt32(seq.Slice(offset), out var trailerLen);
            offset += 4; // reserved

            // don't move the offset. It is a "peek" to determinate the entry type
            // (entryType & 0x80) == 0 is standard entry
            // (entryType & 0x80) != 0 is compress entry
            WireFormatting.ReadByte(seq.Slice(offset), out var entryType);

            var isSubBatch = (entryType & 0x80) != 0;


            var data = seq.Slice(offset, dataLen);


            offset += (int) dataLen;
            chunk = new Chunk(magicVersion, numEntries, numRecords, timestamp, epoch, chunkId, crc, data,
                isSubBatch);

            return offset;
        }
    }
}