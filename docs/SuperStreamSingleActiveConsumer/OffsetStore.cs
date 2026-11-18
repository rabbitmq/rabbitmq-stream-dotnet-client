using System.Collections.Concurrent;
using RabbitMQ.Stream.Client;

namespace SuperStreamSingleActiveConsumer;

/// <summary>
/// Simulates an external storage (a database, Redis, a file...) where the offsets are saved.
/// Here it is just a thread-safe in-memory map: partition name -> last consumed offset.
/// </summary>
public class OffsetStore
{
    private readonly ConcurrentDictionary<string, ulong> _offsets = new();

    public void Store(string stream, ulong lastConsumedOffset) => _offsets[stream] = lastConsumedOffset;

    /// <summary>
    /// The offset to restart from: the last stored offset + 1, or the first message if nothing is stored yet.
    /// </summary>
    public IOffsetType RestartOffset(string stream) =>
        _offsets.TryGetValue(stream, out var last) ? new OffsetTypeOffset(last + 1) : new OffsetTypeFirst();
}
