<h1 style="text-align:center;">RabbitMQ client for the stream protocol</h1>

---
<div style="text-align:center;">

[![Build Status](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions/workflows/main.yaml/badge.svg)](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
[![codecov](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client/branch/main/graph/badge.svg?token=OIA04ZQD79)](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client)
</div>

<h2>Table of content</h2>


---
- [Overview](#overview)
- [Installing via NuGet](#installing-via-nuget)
- [Getting started](#getting-started)
  - [Usage](#usage)
    - [Connect](#connect)
    - [Multi Host](#multi-host)
    - [TLS](#tls)
    - [Load Balancer](#load-balancer)
  - [Manage Streams](#manage-streams)
  - [Producer](#producer)
  - [Publish Messages](#publish-messages)
  - [Consume Messages](#consume-messages)
  - [Build from source](#build-from-source)
  - [Project Status](#project-status)

### Overview

Dotnet client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

### Installing via NuGet
The client is [distributed via NuGet](https://www.nuget.org/packages/RabbitMQ.Stream.Client/).
### Getting started
A rapid getting started
```csharp
 var config = new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/"
};
// Connect to the broker 
var system = await StreamSystem.Create(config);

const string stream = "my_first_stream";

// Create the stream. It is important to put some retention policy 
// in this case is 200000 bytes.
await system.CreateStream(new StreamSpec(stream)
{
    MaxLengthBytes = 200000,
});
var producer = await system.CreateProducer(
    new ProducerConfig
    {
        Reference = Guid.NewGuid().ToString(),
        Stream = stream,
        // Here you can receive the messages confirmation
        // it means the message is stored on the server
        ConfirmHandler = conf =>
        {
            Console.WriteLine($"message: {conf.PublishingId} - confirmed");        
        }
    });

// Publish the messages and set the publishingId that
// should be sequential
for (ulong i = 0; i < 100; i++)
{
    var message = new Message(Encoding.UTF8.GetBytes($"hello {i}"));
    await producer.Send(i, message);
}

// not mandatory. Just to show the confirmation
Thread.Sleep(1000);

// Create a consumer
var consumer = await system.CreateConsumer(
    new ConsumerConfig
    {
        Reference = Guid.NewGuid().ToString(),
        Stream = stream,
        // Consume the stream from the beginning 
        // See also other OffsetSpec 
        OffsetSpec = new OffsetTypeFirst(),
        // Receive the messages
        MessageHandler = async (consumer, ctx, message) =>
        {
            Console.WriteLine($"message: {Encoding.Default.GetString(message.Data.Contents.ToArray())} - consumed");
            await Task.CompletedTask;
        }
    });
Console.WriteLine($"Press to stop");
Console.ReadLine();

await producer.Close();
await consumer.Close();
await system.DeleteStream(stream);
await system.Close();
```

### Usage

---

### Connect

```csharp
var config = new StreamSystemConfig
{
    UserName = "myuser",
    Password = "mypassword",
    VirtualHost = "myhost",
    Endpoints = new List<EndPoint> {new IPEndPoint(IPAddress.Parse("<<brokerip>>"), 5552)},
}
```

### Multi Host
// TODO

### Tls
```csharp
var config = new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    Ssl = new SslOption()
    {
        Enabled = true                    
},
```

### Load Balancer
```csharp

var lbAddressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("<<loadBalancerIP>>"), 5552));
var config = new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    AddressResolver = lbAddressResolver,
    Endpoints = new List<EndPoint> {addressResolver.EndPoint},
}    
```
### Manage Streams

```csharp
await system.CreateStream(new StreamSpec(stream));
```

`system.CreateStream` is idempotent: trying to re-create a stream with the same name and same properties (e.g. maximum size) will not throw an exception. <br/>
In other words, you can be sure the stream has been created once `system.CreateStream` returns. <br/>
Note it is not possible to create a stream with the same name as an existing stream but with different properties. Such a request will result in an exception.

It is possible to set up the retention policy when creating the stream, based on size or time:<br/>
```csharp
await system.CreateStream(new StreamSpec(stream)
{
    MaxLengthBytes = 200000,
    MaxAge = TimeSpan.FromHours(8),
    
});
```

Set a policy is highly recommended. 

### Producer
A Producer instance is created from the `System`.
```csharp
var producer = await system.CreateProducer(
    new ProducerConfig
    {
        Stream = "my_stream",
    });
```
Consider a Producer instance like a long-lived object, do not create one to send just one message.

| Parameter               | Description                            | Default                        |
|-------------------------|----------------------------------------|--------------------------------|
| Stream                  | The stream to publish to.              | No default, mandatory setting. | 
| Reference               | The logical name of the producer.      | null (no deduplication)        | 
 | ClientProvidedName      | Set the TCP Client Name                | `dotnet-stream-producer`       | 
 | ConfirmHandler          | Handler with confirmed messages        | It is an event                 |
 | ConnectionClosedHandler | Event when the client is disconnected  | It is an event                 | 
 | MaxInFlight             | Max Number of messages before send     | 1000                           | 

### Publish Messages

Standard publish: <br/>
```csharp
    var message = new Message(Encoding.UTF8.GetBytes($"hello {i}"));
    await producer.Send(i, message);
```

Sub Entries Batching:<br/>
A sub-entry is one "slot" in a publishing frame, meaning outbound messages are not only batched in publishing frames, but in sub-entries as well. Use this feature to increase throughput at the cost of increased latency.

`CompressionType.None` and `CompressionType.GZip` codecs compression are built-in.

```csharp
var subEntryMessages = List<Messages>();
for (var i = 1; i <= 500; i++)
{
    var message = new Message(Encoding.UTF8.GetBytes($"SubBatchMessage_{i}"));
    subEntryMessages.Add(message);
}

await producer.Send(1, subEntryMessages, CompressionType.Gzip);
messages.Clear();
```

Note:<br/>
`CompressionType.Lz4`,`CompressionType.Snappy`,`CompressionType.Zstd`
are not provided by default.<br>
See the section: "Implement a Custom Compression Codec" for more details.


### Consume Messages
// TODO

### Build from source
// TODO

### Project Status
The client is work in progress. The API(s) could change

