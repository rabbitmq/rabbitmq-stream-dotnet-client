// Example of ConsumerConfig.SubscriptionListener with a super stream consumer.
// The listener is called before every subscription of every partition: the first one and every recovery
// (reconnection, metadata update). ctx.Stream is the partition name, so each partition
// can restart from its own offset, read from an external store (a database, Redis, a file...).
// The external store is simulated here with a dictionary.
// See also the single stream example: ../ConsumerSubscriptionListener

using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

const string SuperStreamName = "subscription-listener-super-stream";

var system = await StreamSystem.Create(new StreamSystemConfig()).ConfigureAwait(false);
await system.CreateSuperStream(new PartitionsSuperStreamSpec(SuperStreamName, 3)).ConfigureAwait(false);

// Simulated external store: partition name -> next offset to consume.
// Here the values are fixed, a real application would load them from its own storage.
var externalStore = new ConcurrentDictionary<string, ulong>
{
    [$"{SuperStreamName}-0"] = 3, [$"{SuperStreamName}-1"] = 4, [$"{SuperStreamName}-2"] = 2
};

var producer = await Producer.Create(new ProducerConfig(system, SuperStreamName)
{
    SuperStreamConfig = new SuperStreamConfig { Routing = message => message.Properties.MessageId.ToString() }
}).ConfigureAwait(false);

for (var i = 0; i < 50; i++)
{
    await producer.Send(new Message(Encoding.UTF8.GetBytes($"message {i}"))
    {
        Properties = new Properties { MessageId = $"key-{i}" }
    }).ConfigureAwait(false);
}

await Task.Delay(1000).ConfigureAwait(false);
await producer.Close().ConfigureAwait(false);

var consumer = await Consumer.Create(new ConsumerConfig(system, SuperStreamName)
{
    IsSuperStream = true,
    Reference = "my-consumer",
    // Used only if the listener doesn't override it.
    OffsetSpec = new OffsetTypeFirst(),
    SubscriptionListener = ctx =>
    {
        // ctx.Stream is the partition: every partition has its own offset.
        // On recovery ctx.OffsetSpec is the last consumed offset + 1: override it only if you need to.
        if (externalStore.TryGetValue(ctx.Stream, out var offset))
        {
            ctx.OffsetSpec = new OffsetTypeOffset(offset);
        }


        Console.WriteLine($"Subscribing to {ctx.Stream} ({ctx.Reference}) with offset {ctx.OffsetSpec}");
        return Task.CompletedTask;
    },
    MessageHandler = (stream, _, context, message) =>
    {
        Console.WriteLine(
            $"[{stream}] offset {context.Offset}: {Encoding.UTF8.GetString(message.Data.Contents)}");

        // uncomment the following line to simulate storing the offset in an external store
//        externalStore[stream] = context.Offset + 1;

        return Task.CompletedTask;
    }
}).ConfigureAwait(false);

Console.WriteLine("Press any key to stop");
Console.ReadKey();
await consumer.Close().ConfigureAwait(false);
await system.DeleteSuperStream(SuperStreamName).ConfigureAwait(false);
await system.Close().ConfigureAwait(false);
