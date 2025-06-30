// See https://aka.ms/new-console-template for more information

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

Console.WriteLine("Manual Credits Example");

var streamSystem = await StreamSystem.Create(
    new StreamSystemConfig()
).ConfigureAwait(false);

const string StreamName = "manual-credits-stream";
await streamSystem.CreateStream(new StreamSpec(StreamName)).ConfigureAwait(false);

var producer = await Producer.Create(
    new ProducerConfig(streamSystem, StreamName)
).ConfigureAwait(false);


var cts = new CancellationTokenSource();
RawConsumer? rawConsumer = null;
_ = Task.Run(async () =>
{
    // simulate a background task that requests credits periodically
    // each 2 seconds request credits from the raw consumer
    while (!cts.IsCancellationRequested)
    {
        await Task.Delay(2000, cts.Token).ConfigureAwait(false);
        if (rawConsumer != null) await rawConsumer.Credits().ConfigureAwait(false);
        else Console.WriteLine("Credits requested...");
    }
}).ConfigureAwait(false);


var consumer = await Consumer.Create(
    new ConsumerConfig(streamSystem, StreamName)
    {
        OffsetSpec = new OffsetTypeFirst(),
        FlowControl = new FlowControl() { Strategy = ConsumerFlowStrategy.ConsumerCredits },
        MessageHandler = (_, sourceConsumer, _, message) =>
        {
            rawConsumer ??= sourceConsumer;
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Data.Contents)}");
            return Task.CompletedTask;
        }
    }
).ConfigureAwait(false);

for (var i = 0; i < 10_000; i++)
{
    var message = new Message(Encoding.UTF8.GetBytes($"Message {i}"));
    await producer.Send(message).ConfigureAwait(false);
}


Console.WriteLine("Press any key to exit...");
Console.ReadKey();
cts.Cancel();
await producer.Close().ConfigureAwait(false);
await consumer.Close().ConfigureAwait(false);
await streamSystem.DeleteStream(StreamName).ConfigureAwait(false);
await streamSystem.Close().ConfigureAwait(false);
