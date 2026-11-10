// Example of ConsumerConfig.SubscriptionListener with a standard (single stream) consumer.
// The listener is called before every subscription: the first one and every recovery
// (reconnection, metadata update). It can override the offset the consumer starts from,
// for example to read it from an external store (a database, Redis, a file...).
// The external store is simulated here with a dictionary.
// See also the super stream example: ../ConsumerSubscriptionListenerSuperStream

using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

const string StreamName = "subscription-listener-stream";

// Simulated external store: stream name -> next offset to consume.
// Here the value is fixed, a real application would load it from its own storage.
var externalStore = new ConcurrentDictionary<string, ulong>
{
    [StreamName] = 10
};

var system = await StreamSystem.Create(new StreamSystemConfig()).ConfigureAwait(false);
await system.CreateStream(new StreamSpec(StreamName)).ConfigureAwait(false);

var producer = await Producer.Create(new ProducerConfig(system, StreamName)).ConfigureAwait(false);
for (var i = 0; i < 20; i++)
{
    await producer.Send(new Message(Encoding.UTF8.GetBytes($"message {i}"))).ConfigureAwait(false);
}

await Task.Delay(1000).ConfigureAwait(false);
await producer.Close().ConfigureAwait(false);

var consumer = await Consumer.Create(new ConsumerConfig(system, StreamName)
{
    Reference = "my-consumer",
    // Used only if the listener doesn't override it.
    OffsetSpec = new OffsetTypeFirst(),
    SubscriptionListener = ctx =>
    {
        // ctx.Stream and ctx.Reference identify the subscription.
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
        // Update the external store with the next offset to consume.
        // Storing it for each message is not a good practice, it is done here only for demo purposes.
        // uncomment the following line to simulate storing the offset in an external store
        //externalStore[stream] = context.Offset + 1;
        return Task.CompletedTask;
    }
}).ConfigureAwait(false);

// With the store above, only the messages from offset 10 to 19 are printed.
Console.WriteLine("Press any key to stop");
Console.ReadKey();
await consumer.Close().ConfigureAwait(false);
await system.DeleteStream(StreamName).ConfigureAwait(false);
await system.Close().ConfigureAwait(false);
