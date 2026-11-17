// Initial Offset Example
// Requires RabbitMQ 4.4.0 or more.
//
// By default the first message of a stream has offset 0.
// With the "stream-initial-offset" argument a stream can start at an arbitrary offset,
// for example to continue a sequence of offsets after migrating data from another system.

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

Console.WriteLine("Initial Offset Example");

var streamSystem = await StreamSystem.Create(
    new StreamSystemConfig()
).ConfigureAwait(false);

const string StreamName = "initial-offset-stream";
const ulong InitialOffset = 1_000_000;

// The initial offset must be between 0 and StreamSpec.MaxInitialOffset (2^62 - 1),
// otherwise an ArgumentOutOfRangeException is thrown.
await streamSystem.CreateStream(new StreamSpec(StreamName)
{
    InitialOffset = InitialOffset
}).ConfigureAwait(false);

var producer = await Producer.Create(
    new ProducerConfig(streamSystem, StreamName)
).ConfigureAwait(false);

var received = new TaskCompletionSource();
var consumer = await Consumer.Create(
    new ConsumerConfig(streamSystem, StreamName)
    {
        OffsetSpec = new OffsetTypeFirst(),
        MessageHandler = (_, _, context, message) =>
        {
            // the first message has offset 1_000_000 instead of 0
            Console.WriteLine(
                $"Received: {Encoding.UTF8.GetString(message.Data.Contents)} at offset {context.Offset}");
            received.TrySetResult();
            return Task.CompletedTask;
        }
    }
).ConfigureAwait(false);

await producer.Send(new Message(Encoding.UTF8.GetBytes("first message"))).ConfigureAwait(false);
await received.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

await producer.Close().ConfigureAwait(false);
await consumer.Close().ConfigureAwait(false);
await streamSystem.DeleteStream(StreamName).ConfigureAwait(false);
await streamSystem.Close().ConfigureAwait(false);
