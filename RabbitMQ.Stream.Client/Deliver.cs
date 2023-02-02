﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    //Deliver => Key Version SubscriptionId OsirisChunk
    //   Key => uint16 // 8
    //   Version => uint32
    //   SubscriptionId => uint8
    public readonly struct Deliver : ICommand
    {
        private readonly byte subscriptionId;
        public const ushort Key = 8;
        public int SizeNeeded => throw new NotImplementedException();

        private Deliver(byte subscriptionId, Chunk chunk)
        {
            this.subscriptionId = subscriptionId;
            Chunk = chunk;
        }

        public Chunk Chunk { get; }

        public byte SubscriptionId => subscriptionId;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out Deliver command)
        {
            var reader = new SequenceReader<byte>(frame);
            var offset = WireFormatting.ReadUInt16(ref reader, out _);
            offset += WireFormatting.ReadUInt16(ref reader, out _);
            offset += WireFormatting.ReadByte(ref reader, out var subscriptionId);
            offset += Chunk.Read(reader.Sequence.Slice(offset), out var chunk);
            command = new Deliver(subscriptionId, chunk);
            return offset;
        }
    }

    internal readonly struct SubEntryChunk
    {
        private readonly byte compressValue;

        private SubEntryChunk(byte compress,
            ushort numRecordsInBatch,
            uint unCompressedDataSize, uint dataLen,
            ReadOnlySequence<byte> data)
        {
            compressValue = compress;
            NumRecordsInBatch = numRecordsInBatch;
            UnCompressedDataSize = unCompressedDataSize;
            DataLen = dataLen;
            Data = data;
        }

        public CompressionType CompressionType => (CompressionType)compressValue;

        public ushort NumRecordsInBatch { get; }

        public uint UnCompressedDataSize { get; }

        public uint DataLen { get; }
        public ReadOnlySequence<byte> Data { get; }

        internal static int Read(ref SequenceReader<byte> reader, byte entryType, out SubEntryChunk subEntryChunk)
        {
            var offset = WireFormatting.ReadUInt16(ref reader, out var numRecordsInBatch);
            offset += WireFormatting.ReadUInt32(ref reader, out var unCompressedDataSize);
            offset += WireFormatting.ReadUInt32(ref reader, out var dataLen);
            // Determinate what kind of the compression it is using
            // See Compress:CompressMode
            var compress = (byte)((byte)(entryType & 0x70) >> 4);
            offset++;

            // Data contains the subEntryChunk information
            // We need to pass it to the subEntryChunk that will decode the information
            var data = reader.Sequence.Slice(reader.Consumed, dataLen);
            subEntryChunk =
                new SubEntryChunk(compress, numRecordsInBatch, unCompressedDataSize, dataLen, data);
            offset += (int)dataLen;
            // Here we need to advance the reader to the datalen
            // Since Data is passed to the subEntryChunk.
            reader.Advance(dataLen);
            return offset;
        }
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
            ReadOnlySequence<byte> data)
        {
            MagicVersion = magicVersion;
            NumEntries = numEntries;
            NumRecords = numRecords;
            Timestamp = timestamp;
            Epoch = epoch;
            ChunkId = chunkId;
            Crc = crc;
            Data = data;
        }

        public byte MagicVersion { get; }

        public ushort NumEntries { get; }
        public uint NumRecords { get; }
        public long Timestamp { get; }
        public ulong Epoch { get; }
        public ulong ChunkId { get; }
        public int Crc { get; }
        public ReadOnlySequence<byte> Data { get; }

        internal static int Read(ReadOnlySequence<byte> frame, out Chunk chunk)
        {
            var reader = new SequenceReader<byte>(frame);
            var offset = WireFormatting.ReadByte(ref reader, out var magicVersion);
            offset += WireFormatting.ReadByte(ref reader, out _);
            offset += WireFormatting.ReadUInt16(ref reader, out var numEntries);
            offset += WireFormatting.ReadUInt32(ref reader, out var numRecords);
            offset += WireFormatting.ReadInt64(ref reader, out var timestamp);
            offset += WireFormatting.ReadUInt64(ref reader, out var epoch);
            offset += WireFormatting.ReadUInt64(ref reader, out var chunkId);
            offset += WireFormatting.ReadInt32(ref reader, out var crc);
            offset += WireFormatting.ReadUInt32(ref reader, out var dataLen);
            offset += WireFormatting.ReadUInt32(ref reader, out _);
            // offset += 4; // reserved
            offset += WireFormatting.ReadUInt32(ref reader, out _); // reserved
            var data = reader.Sequence.Slice(reader.Consumed, dataLen);
            offset += (int)dataLen;
            chunk = new Chunk(magicVersion, numEntries, numRecords, timestamp, epoch, chunkId, crc, data);
            return offset;
        }
    }
}
