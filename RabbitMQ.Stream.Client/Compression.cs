using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace RabbitMQ.Stream.Client
{
    // Compress Mode for sub-batching publish
    public enum CompressionMode : byte
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
    // used by SubBatchPublish to publish the messages
    public interface ICompressionCodec
    {
        void Compress(List<Message> messages);
        public int Write(Span<byte> span);
        public int CompressedSize { get; }
        public int UnCompressedSize { get; }

        public int MessagesCount { get; }

        public CompressionMode CompressionMode { get; }

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
                offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint) msg.Size);
                offset += msg.Write(span.Slice(offset));
            }

            return offset;
        }

        public int CompressedSize { get; private set; }
        public int UnCompressedSize { get; private set; }
        public int MessagesCount => rMessages.Count;
        public CompressionMode CompressionMode => CompressionMode.None;

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
        public CompressionMode CompressionMode => CompressionMode.Gzip;


        public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
        {
            using var sourceMemoryStream = new MemoryStream(source.ToArray(), 0, (int) dataLen);
            using var resultMemoryStream = new MemoryStream((int) unCompressedDataSize);
            using var gZipStream = new GZipStream(sourceMemoryStream, System.IO.Compression.CompressionMode.Decompress);
            gZipStream.CopyTo(resultMemoryStream);
            gZipStream.Flush();
            return new ReadOnlySequence<byte>(resultMemoryStream.ToArray());
        }
    }

    /// <summary>
    /// StreamCompressionCodecs: Global class to register/unregister the compress codec
    /// GZip and None compression are provided by default
    /// </summary>
    public static class StreamCompressionCodecs
    {
        private static readonly Dictionary<CompressionMode, Type> AvailableCompressCodecs =
            new()
            {
                {CompressionMode.Gzip, typeof(GzipCompressionCodec)},
                {CompressionMode.None, typeof(NoneCompressionCodec)},
            };

        public static void RegisterCodec<T>(CompressionMode compressionMode) where T : ICompressionCodec, new()
        {
            if (AvailableCompressCodecs.ContainsKey(compressionMode))
            {
                throw new CodecAlreadyExistException($"codec for {compressionMode} already exist.");
            }

            AvailableCompressCodecs.Add(compressionMode, typeof(T));
        }

        public static void UnRegisterCodec(CompressionMode compressionMode)
        {
            if (AvailableCompressCodecs.ContainsKey(compressionMode))
            {
                AvailableCompressCodecs.Remove(compressionMode);
            }
        }

        public static ICompressionCodec GetCompressionCodec(CompressionMode compressionMode)
        {
            if (!AvailableCompressCodecs.ContainsKey(compressionMode))
            {
                throw new CodecNotFoundException($"codec for {compressionMode} not found");
            }

            return (ICompressionCodec) Activator.CreateInstance(AvailableCompressCodecs[compressionMode]);
        }
    }

    public static class CompressionHelper
    {
        public static ICompressionCodec Compress(List<Message> messages, CompressionMode compressionMode)
        {
            if (messages.Count > ushort.MaxValue)
            {
                throw new OutOfBoundsException($"List out of limits: 0-{ushort.MaxValue}");
            }

            var codec
                = StreamCompressionCodecs.GetCompressionCodec(compressionMode);
            codec.Compress(messages);
            return codec;
        }

        public static ReadOnlySequence<byte> UnCompress(CompressionMode compressionMode,
            ReadOnlySequence<byte> source, uint dataLen,
            uint unCompressedDataSize)
        {
            var codec = StreamCompressionCodecs.GetCompressionCodec(compressionMode);
            return codec.UnCompress(source, dataLen, unCompressedDataSize);
        }
    }


    public class OutOfBoundsException : Exception
    {
        public OutOfBoundsException(string s) :
            base(s)
        {
        }
    }

    public class CodecNotFoundException : Exception
    {
        public CodecNotFoundException(string s) :
            base(s)
        {
        }
    }

    public class CodecAlreadyExistException : Exception
    {
        public CodecAlreadyExistException(string s) :
            base(s)
        {
        }
    }
}