<h1 align="center">RabbitMQ client for the stream protocol</h1>

---
<div align="center">

![tests](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions/workflows/nuget.yml//badge.svg)	
[![codecov](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client/branch/main/graph/badge.svg?token=OIA04ZQD79)](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client)

</div>

## Dependency (NuGet Artifact)

The client is [distributed via NuGet](https://www.nuget.org/packages/RabbitMQ.Stream.Client/).


## Getting started

```csharp
var config = new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
};

//connect to the broker 
var system = await StreamSystem.Create(config);


const string stream = "my_first_stream";

// create the stream
await system.CreateStream(new StreamSpec(stream));


// Create the consumer 
var consumer = await system.CreateConsumer(
    new ConsumerConfig
    {
        Reference = Guid.NewGuid().ToString(),
        Stream = stream,
        MessageHandler = async (consumer, ctx, message) =>
        {
            Console.WriteLine($"message: {Encoding.Default.GetString(message.Data.Contents.ToArray())}");
            await Task.CompletedTask;
        }
    });

// Create the producer
var producer = await system.CreateProducer(
    new ProducerConfig
    {
        Reference = Guid.NewGuid().ToString(),
        Stream = stream,
        ConfirmHandler = conf => { },
        ConnectionClosedHandler = async s =>
        {
            Console.WriteLine($"producer closed {s}");
            await Task.CompletedTask;
        }
    });

// publish messages
for (ulong i = 0; i < 100; i++)
{
    var readonlySequence = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes($"hello {i}"));
    var message = new Message(new Data(readonlySequence));
    await producer.Send(i, message);
}

Console.WriteLine($"Press to stop");
Console.ReadLine();
           
await producer.Close();
await consumer.Close();
await system.DeleteStream(stream);
await system.Close();

Console.WriteLine($"Done");
```

## Project Status	
The client is work in progress. The API(s) could change

