using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    // Compress Mode for sub-batching publish
    // see the test: UnitTests:CompressUnCompressShouldHaveTheSize/0
    // it already tests all the compress mode(s)
    // once all the other kind will be implemented the tests 
    // must pass.
    public enum CompressMode : byte
    {
        None = 0,
        Gzip = 1,
        // Not implemented yet.
        // Snappy = 2,
        // Lz4 = 3,
        // Zstd = 4,
    }

    // Interface for compress the messages
    // used on the SubBatchPublish to publish the message
    // using the compression
    public interface ICompress
    {
        void Compress(List<Message> messages);
        public int Write(Span<byte> span);
        public int CompressedSize { get; }
        public int UnCompressedSize { get; }

        public int MessagesCount { get; }


        public CompressMode CompressMode { get; }
    }

    internal class NoneCompress : ICompress
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
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            return offset;
        }

        public int CompressedSize { get; private set; }
        public int UnCompressedSize { get; private set; }
        public int MessagesCount => rMessages.Count;
        public CompressMode CompressMode => CompressMode.None;
    }

    internal class GzipCompress : ICompress
    {
        private ReadOnlySequence<byte> compressedReadOnlySequence;

        public void Compress(List<Message> messages)
        {
            MessagesCount = messages.Count;
            UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
            using var result = new MemoryStream();
            using var gZipStream = new GZipStream(result, CompressionLevel.Optimal);
            var span = new Span<byte>(new byte[UnCompressedSize]);
            var offset = 0;
            foreach (var msg in messages)
            {
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            gZipStream.Write(span);
            gZipStream.Flush();
            compressedReadOnlySequence = new ReadOnlySequence<byte>(result.ToArray());
        }

        public int Write(Span<byte> span)
        {
            return WireFormatting.Write(span, compressedReadOnlySequence);
        }

        public int CompressedSize => (int) compressedReadOnlySequence.Length;
        public int UnCompressedSize { get; private set; }
        public int MessagesCount { get; private set; }
        public CompressMode CompressMode => CompressMode.Gzip;
    }

    public static class CompressHelper
    {
        private static readonly Dictionary<CompressMode, ICompress> CompressesList =
            new()
            {
                {CompressMode.Gzip, new GzipCompress()},
                {CompressMode.None, new NoneCompress()},
            };

        public static ICompress Compress(List<Message> messages, CompressMode compressionMode)
        {
            if (messages.Count > ushort.MaxValue)
            {
                throw new OutOfBoundsException($"List out of limits: 0-{ushort.MaxValue}");
            }
            var result = CompressesList[compressionMode];
            result.Compress(messages);
            return result;
        }

        private static readonly Dictionary<CompressMode, IUnCompress> UnCompressesList =
            new()
            {
                {CompressMode.None, new NoneUnCompress()},
                {CompressMode.Gzip, new GzipUnCompress()},
            };

        public static ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen,
            uint unCompressedDataSize,
            CompressMode compressionMode)
        {
            var result = UnCompressesList[compressionMode];
            return result.UnCompress(source, dataLen, unCompressedDataSize);
        }
    }

    // UnCompress Section
    public interface IUnCompress
    {
        ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize);
    }

    internal class NoneUnCompress : IUnCompress
    {
        public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
        {
            return source;
        }
    }


    internal class GzipUnCompress : IUnCompress
    {
        public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
        {
            using var sourceMemoryStream = new MemoryStream(source.ToArray(), 0, (int) dataLen);
            using var resultMemoryStream = new MemoryStream((int) unCompressedDataSize);
            using var gZipStream = new GZipStream(sourceMemoryStream, CompressionMode.Decompress);
            gZipStream.CopyTo(resultMemoryStream);
            gZipStream.Flush();
            return new ReadOnlySequence<byte>(resultMemoryStream.ToArray());
        }
    }
    
    
    
    public class OutOfBoundsException : Exception
    {
        public OutOfBoundsException(string s) :
            base(s)
        {
        }
    }
}