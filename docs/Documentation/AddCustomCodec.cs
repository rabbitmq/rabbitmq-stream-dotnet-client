// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries..

using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using K4os.Compression.LZ4.Streams;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Documentation;

// tag::lz4-i-compression-codec[]
class StreamLz4Codec : ICompressionCodec // <1>
{



    private ReadOnlySequence<byte> _compressedReadOnlySequence;
    public void Compress(List<Message> messages)
    {
        MessagesCount = messages.Count;
        UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
        var messagesSource = new Span<byte>(new byte[UnCompressedSize]);
        var offset = 0;
        foreach (var msg in messages)
        {
            offset += WriteUInt32(messagesSource.Slice(offset), (uint)msg.Size);
            offset += msg.Write(messagesSource.Slice(offset));
        }

        using var source = new MemoryStream(messagesSource.ToArray());
        using var destination = new MemoryStream();
        var settings = new LZ4EncoderSettings {ChainBlocks = false};
        using (var target = LZ4Stream.Encode(destination, settings, false))
        {
            source.CopyTo(target);
        }

        _compressedReadOnlySequence = new ReadOnlySequence<byte>(destination.ToArray());
    }

    public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
    {
        using var target = new MemoryStream();
        using (var sourceDecode = LZ4Stream.Decode(new MemoryStream(source.ToArray())))
        {
            sourceDecode.CopyTo(target);
        }
        return new ReadOnlySequence<byte>(target.ToArray());
    }

// end::lz4-i-compression-codec[]
    private static int WriteUInt32(Span<byte> span, uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        return 4;
    }

    private static int Write(Span<byte> span, ReadOnlySequence<byte> msg)
    {
        msg.CopyTo(span);
        return (int)msg.Length;
    }

    
    public int Write(Span<byte> span)
    {
        return Write(span, _compressedReadOnlySequence);
    }

    public int CompressedSize => (int)_compressedReadOnlySequence.Length;

    public int UnCompressedSize { get; private set; }
    public int MessagesCount { get; private set; }

    public CompressionType CompressionType => CompressionType.Lz4;

  
}

public class AddCustomCodec
{
    public static async Task AddLz4Codec()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        // tag::lz4-register-codec[]
        StreamCompressionCodecs.RegisterCodec<StreamLz4Codec>(CompressionType.Lz4); // <1>

        var producer = await Producer.Create(
            new ProducerConfig(
                streamSystem,
                "my-stream") { }
        ).ConfigureAwait(false);

        var message = new Message(Encoding.UTF8.GetBytes("hello"));
        var list = new List<Message> {message, message, message}; 
        await producer.Send(list, CompressionType.Lz4).ConfigureAwait(false); // <2>
        // end::lz4-register-codec[]
        await producer.Close().ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
    }
}
