using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStreamSingleActiveConsumer;

public static class SuperStreamSacConsumer
{
    public static async Task Start(StreamSystem system, ILogger logger, ILogger<Consumer> consumerLogger)
    {
        // Simulated external storage, shared by the listeners and the message handler.
        var store = new OffsetStore();
        var counters = new ConcurrentDictionary<string, long>();

        var consumer = await Consumer.Create(new ConsumerConfig(system, Program.SuperStreamName)
        {
            IsSuperStream = true,
            IsSingleActiveConsumer = true, // the reference is mandatory with single active consumer
            Reference = "my-sac-super-stream-consumer",
            // Used only when the store has nothing for the partition.
            OffsetSpec = new OffsetTypeFirst(),

            // Called before EVERY subscription of EVERY partition: the first one and each recovery
            // (reconnection, metadata update). ctx.Stream is the partition name.
            SubscriptionListener = ctx =>
            {
                var before = ctx.OffsetSpec;
                ctx.OffsetSpec = store.RestartOffset(ctx.Stream);
                logger.LogInformation(
                    "[SubscriptionListener] partition {Stream}, reference {Reference}: " +
                    "library offset {Before} -> the consumer is going to restart from {After}",
                    ctx.Stream, ctx.Reference, before, ctx.OffsetSpec);
                return Task.CompletedTask;
            },

            // Called when the broker changes the status of this consumer for a partition (single active consumer only).
            // It runs in the socket thread: keep it fast and safe. The returned offset is used only when promoted to active.
            ConsumerUpdateListener = (reference, stream, isActive) =>
            {
                if (!isActive)
                {
                    logger.LogInformation(
                        "[ConsumerUpdateListener] {Reference} is now INACTIVE on {Stream}, it stops receiving messages",
                        reference, stream);
                    return Task.FromResult<IOffsetType>(new OffsetTypeNext());
                }

                var offset = store.RestartOffset(stream);
                logger.LogInformation(
                    "[ConsumerUpdateListener] {Reference} is now ACTIVE on {Stream}: the consumer is going to restart from {Offset}",
                    reference, stream, offset);
                return Task.FromResult(offset);
            },

            MessageHandler = (stream, _, context, message) =>
            {
                // Store the last consumed offset in the "external" storage.
                // Storing at every message is fine for an in-memory map; a real storage would batch it.
                store.Store(stream, context.Offset);

                var count = counters.AddOrUpdate(stream, 1, (_, c) => c + 1);
                if (count % 100_000 == 0)
                {
                    logger.LogInformation("[{Stream}] consumed {Count} messages, last offset {Offset}, last message '{Message}'",
                        stream, count, context.Offset, Encoding.UTF8.GetString(message.Data.Contents));
                }

                return Task.CompletedTask;
            }
        }, consumerLogger).ConfigureAwait(false);

        Console.WriteLine("Consumer is running. Start more instances to see the single active consumer switch. Press [enter] to exit.");
        Console.ReadLine();
        await consumer.Close().ConfigureAwait(false);
    }
}
