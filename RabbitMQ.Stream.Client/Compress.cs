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
    
}