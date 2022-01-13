using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public enum CompressMode : byte
    {
        None = 0,
        Gzip = 1,
        Snappy = 2,
        Lz4 = 3,
        Zstd = 4,
    }

    public interface ICompress
    {
        void Compress(List<Message> messages);
        public int Write(Span<byte> span);
        public int CompressedSize { get; }
        public int UnCompressedSize { get; }
        public int MessagesCount { get; }

        public CompressMode CompressMode { get; }
    }

    internal class None : ICompress
    {
        private List<Message> rMessages;

        public void Compress(List<Message> messages)
        {
            rMessages = messages;
            UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
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

    internal class Gzip : ICompress
    {
        private ReadOnlySequence<byte> compressedReadOnlySequence;

        public Gzip()
        {
            UnCompressedSize = 0;
        }

        public void Compress(List<Message> messages)
        {
            MessagesCount = messages.Count;
            UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
            var result = new MemoryStream();
            var gZipStream = new GZipStream(result, CompressionLevel.Optimal);
            try
            {
                Span<byte> span = new Span<byte>(new byte[UnCompressedSize]);
                var offset = 0;
                foreach (var msg in messages)
                {
                    offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                    offset += msg.Write(span.Slice(offset));
                }

                gZipStream.Write(span);
                gZipStream.Flush();
            }
            finally
            {
                gZipStream.Close();
                compressedReadOnlySequence = new ReadOnlySequence<byte>(result.ToArray());
                result.Close();
            }
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

    internal static class CompressHelper
    {
        private static readonly Dictionary<CompressMode, ICompress> Compresses =
            new()
            {
                {CompressMode.Gzip, new Gzip()},
                {CompressMode.None, new None()},
            };

        public static ICompress Compress(List<Message> messages, CompressMode compressionMode)
        {
            var result = Compresses[compressionMode];
            result.Compress(messages);
            return result;
        }
    }
}