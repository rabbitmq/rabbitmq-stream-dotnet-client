public class StreamLz4Codec : ICompressionCodec
{
    private ReadOnlySequence<byte> compressedReadOnlySequence;

    private static int WriteUInt32(Span<byte> span, uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        return 4;
    }

    private static int Write(Span<byte> span, ReadOnlySequence<byte> msg)
    {
        msg.CopyTo(span);
        return (int) msg.Length;
    }

    public void Compress(List<Message> messages)
    {
        MessagesCount = messages.Count;
        UnCompressedSize = messages.Sum(msg => 4 + msg.Size);
        var messagesSource = new Span<byte>(new byte[UnCompressedSize]);
        var offset = 0;
        foreach (var msg in messages)
        {
            offset += WriteUInt32(messagesSource.Slice(offset), (uint) msg.Size);
            offset += msg.Write(messagesSource.Slice(offset));
        }

        using var source = new MemoryStream(messagesSource.ToArray());
        using var destination = new MemoryStream();
        var settings = new LZ4EncoderSettings
        {
            ChainBlocks = false
        };
        using (var target = LZ4Stream.Encode(destination, settings, false))
        {
            source.CopyTo(target);
        }
        compressedReadOnlySequence = new ReadOnlySequence<byte>(destination.ToArray());
    }

    public int Write(Span<byte> span)
    {
        return Write(span, compressedReadOnlySequence);
    }

    public int CompressedSize => (int) compressedReadOnlySequence.Length;

    public int UnCompressedSize { get; private set; }
    public int MessagesCount { get; private set; }

    public CompressionType CompressionType => CompressionType.Lz4;

    public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
    {
        using var target = new MemoryStream();
        using (var sourceDecode = LZ4Stream.Decode(new MemoryStream(source.ToArray())))
        {
            sourceDecode.CopyTo(target);
        }

        return new ReadOnlySequence<byte>(target.ToArray());
    }
}