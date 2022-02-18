// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace RabbitMQ.Stream.Client
{
    // Compression Type for sub-entry publish
    public enum CompressionType : byte
    {
        // builtin compression
        None = 0,
        Gzip = 1,

        // Not implemented by default.
        // It is possible to add custom codec with StreamCompressionCodecs
        Snappy = 2,
        Lz4 = 3,
        Zstd = 4,
    }

    // Interface for Compress/unCompress the messages
    // used by SubEntryPublish to publish the messages
    public interface ICompressionCodec
    {
        void Compress(List<Message> messages);
        public int Write(Span<byte> span);
        public int CompressedSize { get; }
        public int UnCompressedSize { get; }

        public int MessagesCount { get; }

        public CompressionType CompressionType { get; }

        ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize);
    }

    public class NoneCompressionCodec : ICompressionCodec
    {
        private List<Message> rMessages;

        public void Compress(List<Message> messages)
        {
            rMessages = messages;
            UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
            // since the buffer is not compressed CompressedSize is ==UnCompressedSize 
            CompressedSize = UnCompressedSize;
        }

        public int Write(Span<byte> span)
        {
            var offset = 0;
            foreach (var msg in rMessages)
            {
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            return offset;
        }

        public int CompressedSize { get; private set; }
        public int UnCompressedSize { get; private set; }
        public int MessagesCount => rMessages.Count;
        public CompressionType CompressionType => CompressionType.None;

        public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen,
            uint unCompressedDataSize)
        {
            return source;
        }
    }

    public class GzipCompressionCodec : ICompressionCodec
    {
        private ReadOnlySequence<byte> compressedReadOnlySequence;

        public void Compress(List<Message> messages)
        {
            MessagesCount = messages.Count;
            UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
            var span = new Span<byte>(new byte[UnCompressedSize]);
            var offset = 0;
            foreach (var msg in messages)
            {
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            using var compressedMemory = new MemoryStream();
            using (var gZipStream =
                   new GZipStream(compressedMemory, CompressionMode.Compress))
            {
                gZipStream.Write(span);
            }

            compressedReadOnlySequence = new ReadOnlySequence<byte>(compressedMemory.ToArray());
        }

        public int Write(Span<byte> span)
        {
            return WireFormatting.Write(span, compressedReadOnlySequence);
        }

        public int CompressedSize => (int)compressedReadOnlySequence.Length;
        public int UnCompressedSize { get; private set; }
        public int MessagesCount { get; private set; }
        public CompressionType CompressionType => CompressionType.Gzip;

        public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
        {
            using var sourceMemoryStream = new MemoryStream(source.ToArray(), 0, (int)dataLen);
            using var resultMemoryStream = new MemoryStream((int)unCompressedDataSize);
            using (var gZipStream =
                   new GZipStream(sourceMemoryStream, CompressionMode.Decompress))
            {
                gZipStream.CopyTo(resultMemoryStream);
            }

            return new ReadOnlySequence<byte>(resultMemoryStream.ToArray());
        }
    }

    /// <summary>
    /// StreamCompressionCodecs: Global class to register/unregister the compress codec
    /// GZip and None compression are provided by default
    /// </summary>
    public static class StreamCompressionCodecs
    {
        private static readonly Dictionary<CompressionType, Type> AvailableCompressCodecs =
            new()
            {
                { CompressionType.Gzip, typeof(GzipCompressionCodec) },
                { CompressionType.None, typeof(NoneCompressionCodec) },
            };

        public static void RegisterCodec<T>(CompressionType compressionType) where T : ICompressionCodec, new()
        {
            if (AvailableCompressCodecs.ContainsKey(compressionType))
            {
                throw new CodecAlreadyExistException($"codec for {compressionType} already exist.");
            }

            AvailableCompressCodecs.Add(compressionType, typeof(T));
        }

        public static void UnRegisterCodec(CompressionType compressionType)
        {
            if (AvailableCompressCodecs.ContainsKey(compressionType))
            {
                AvailableCompressCodecs.Remove(compressionType);
            }
        }

        public static ICompressionCodec GetCompressionCodec(CompressionType compressionType)
        {
            if (!AvailableCompressCodecs.ContainsKey(compressionType))
            {
                throw new CodecNotFoundException($"codec for {compressionType} not found");
            }

            return (ICompressionCodec)Activator.CreateInstance(AvailableCompressCodecs[compressionType]);
        }
    }

    public static class CompressionHelper
    {
        public static ICompressionCodec Compress(List<Message> messages, CompressionType compressionType)
        {
            if (messages.Count > ushort.MaxValue)
            {
                throw new OutOfBoundsException($"List out of limits: 0-{ushort.MaxValue}");
            }

            var codec
                = StreamCompressionCodecs.GetCompressionCodec(compressionType);
            codec.Compress(messages);
            return codec;
        }

        public static ReadOnlySequence<byte> UnCompress(CompressionType compressionType,
            ReadOnlySequence<byte> source, uint dataLen,
            uint unCompressedDataSize)
        {
            var codec = StreamCompressionCodecs.GetCompressionCodec(compressionType);
            return codec.UnCompress(source, dataLen, unCompressedDataSize);
        }
    }

    public class OutOfBoundsException : Exception
    {
        public OutOfBoundsException(string s)
            : base(s)
        {
        }
    }

    public class CodecNotFoundException : Exception
    {
        public CodecNotFoundException(string s)
            : base(s)
        {
        }
    }

    public class CodecAlreadyExistException : Exception
    {
        public CodecAlreadyExistException(string s)
            : base(s)
        {
        }
    }
}
