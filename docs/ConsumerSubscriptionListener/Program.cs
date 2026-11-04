// Example of ConsumerConfig.SubscriptionListener.
// The listener is called before every subscription: the first one and every recovery
// (reconnection, metadata update). It can override the offset the consumer starts from,
// for example to read it from an external store. It works for standard and super stream consumers.

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

const string StreamName = "subscription-listener-stream";

var system = await StreamSystem.Create(new StreamSystemConfig()).ConfigureAwait(false);
await system.CreateStream(new StreamSpec(StreamName)).ConfigureAwait(false);

var producer = await Producer.Create(new ProducerConfig(system, StreamName)).ConfigureAwait(false);
for (var i = 0; i < 20; i++)
{
    await producer.Send(new Message(System.Text.Encoding.UTF8.GetBytes($"message {i}"))).ConfigureAwait(false);
}

await Task.Delay(1000).ConfigureAwait(false);
await producer.Close().ConfigureAwait(false);

var consumer = await Consumer.Create(new ConsumerConfig(system, StreamName)
{
    Reference = "my-consumer",
    // Without the listener, OffsetTypeNext would not return the existing messages.
    OffsetSpec = new OffsetTypeFirst(),
    SubscriptionListener = async ctx =>
    {
        // ctx.Stream and ctx.Reference identify the subscription.
       // ctx.OffsetSpec = new OffsetTypeOffset(10); // e.g. an offset loaded from a database
        // On recovery ctx.OffsetSpec is the last consumed offset + 1: change it only if you need to.
        await system.TryQueryOffset(ctx.Reference, ctx.Stream).ConfigureAwait(false); // e.g. to check the last stored offset in a database
        Console.WriteLine($"Subscribing to {ctx.Stream} ({ctx.Reference}) with offset {ctx.OffsetSpec}");
    },
    MessageHandler = async (stream, consumer, context, message) =>
    {
        Console.WriteLine(
            $"[{DateTime.Now}] [{stream}] offset {context.Offset}: {System.Text.Encoding.UTF8.GetString(message.Data.Contents)}");
        await consumer.StoreOffset(context.Offset)
            .ConfigureAwait(
                false); // store offset for each message is not a good practice, here is only for demo purpose
    }
}).ConfigureAwait(false);

Console.WriteLine("Press any key to stop");
Console.ReadKey();
await consumer.Close().ConfigureAwait(false);
await system.DeleteStream(StreamName).ConfigureAwait(false);
await system.Close().ConfigureAwait(false);
