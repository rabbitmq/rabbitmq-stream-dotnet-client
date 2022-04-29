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
    - [Deduplication](#Deduplication)
    - [Consume Messages](#consume-messages)
      - [Offset Types](#offset-types)
      - [Track Offset](#track-offset)
    - [Handle Close](#handle-close)
    - [Handle Metadata Update](#handle-metadata-update)
    - [Reliable](#reliable)
      - [Reliable Producer](#reliable-producer)
    - [Build from source](#build-from-source)
    - [Project Status](#project-status)

### Overview

Dotnet client for [RabbitMQ Stream Queues](https://www.rabbitmq.com/stream.html)

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

```csharp
var config = new StreamSystemConfig
{
    UserName = "myuser",
    Password = "mypassword",
    VirtualHost = "myhost",
    Endpoints = new List<EndPoint>
    {
        new IPEndPoint(IPAddress.Parse("<<brokerip1>>"), 5552),
        new IPEndPoint(IPAddress.Parse("<<brokerip2>>"), 5552),
        new IPEndPoint(IPAddress.Parse("<<brokerip3>>"), 5552)
    },
};
```

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

`system.CreateStream` is idempotent: trying to re-create a stream with the same name and same properties (e.g. maximum
size) will not throw an exception. <br/>
In other words, you can be sure the stream has been created once `system.CreateStream` returns. <br/>
Note it is not possible to create a stream with the same name as an existing stream but with different properties. Such
a request will result in an exception.

It is possible to set up the retention policy when creating the stream, based on size or time:<br/>

```csharp
await system.CreateStream(new StreamSpec(stream)
{
    MaxLengthBytes = 200000,
    MaxAge = TimeSpan.FromHours(8),
});
```

Set a policy is highly recommended.

RabbitMQ does not store the whole stream in a single file, but splits it in segment files. 
This is also used for truncate the stream: when a stream reaches his maximum size, an entire segment file is deleted. For this reason  `MaxLengthBytes` (the max dimension of the entire stream) is usually significantly higher than `MaxSegmentSizeBytes` (the max dimension of a single segment file).
RabbitMQ enforces the retention policy when the current segment has reached its maximum size and is closed in favor of a new one.

```csharp
await system.CreateStream(new StreamSpec(stream)
{
    MaxLengthBytes = 20_000,
    MaxSegmentSizeBytes = 1000
});
```

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

#### Standard publish

```csharp
    var publishingId = 0;
    var message = new Message(Encoding.UTF8.GetBytes("hello"));
    await producer.Send(publishingId, message);
```

`publishingId` must be incremented for each send.

#### Sub Entries Batching
A sub-entry is one "slot" in a publishing frame, meaning outbound messages are not only batched in publishing frames,
but in sub-entries as well. Use this feature to increase throughput at the cost of increased latency.

```csharp
var subEntryMessages = List<Messages>();
for (var i = 1; i <= 500; i++)
{
    var message = new Message(Encoding.UTF8.GetBytes($"SubBatchMessage_{i}"));
    subEntryMessages.Add(message);
}
var publishingId = 1; 
await producer.Send(publishingId, subEntryMessages, CompressionType.Gzip);
messages.Clear();
```

Not all the compressions are implemented by defaults, to avoid to many dependencies.
See the table:

| Compression            | Description    | Provided by client |
|------------------------|----------------|--------------------|
| CompressionType.None   | No compression | yes                |
| CompressionType.GZip   | GZip           | yes                |
| CompressionType.Lz4    | Lz4            | No                 |
| CompressionType.Snappy | Snappy         | No                 |
| CompressionType.Zstd   | Zstd           | No                 |

You can add missing codecs with `StreamCompressionCodecs.RegisterCodec` api.
See [Examples/CompressCodecs](./Examples/CompressCodecs) for `Lz4`,`Snappy` and `Zstd` implementations.

### Deduplication
[See here for more details](https://rabbitmq.github.io/rabbitmq-stream-java-client/snapshot/htmlsingle/#outbound-message-deduplication)
Set a producer reference to enable the deduplication:

```csharp
var producer = await system.CreateProducer(
    new ProducerConfig
    {
        Reference = "my_producer",
        Stream = "my_stream",
    });
```
then:
```csharp
var publishingId = 0;
var message = new Message(Encoding.UTF8.GetBytes($"my deduplicate message {i}"));
await producer.Send(publishingId, message);
```

### Consume Messages
Define a consumer:
```csharp
var consumer = await system.CreateConsumer(
    new ConsumerConfig
    {
        Reference = "my_consumer",
        Stream = stream,
        MessageHandler = async (consumer, ctx, message) =>
        {
            Console.WriteLine(
                $"message: {Encoding.Default.GetString(message.Data.Contents.ToArray())}");
            await Task.CompletedTask;
        }
});
```
### Offset Types
There are five types of Offset and they can be set by the `ConsumerConfig.OffsetSpec` property that must be passed to the Consumer constructor, in the example we use `OffsetTypeFirst`:
```csharp
var consumerOffsetTypeFirst = await system.CreateConsumer(
    new ConsumerConfig
    {
        Reference = "my_consumer_offset_first",
        Stream = stream,
        OffsetSpec = new OffsetTypeFirst(),
        MessageHandler = async (consumer, ctx, message) =>
        {
 
            await Task.CompletedTask;
        }
    });
```
The five types are:
-  First: it takes messages from the first message of the stream. 
```csharp
var offsetTypeFirst = new OffsetTypeFirst();
```
- Last: it takes messages from the last chunk of the stream, i.e. it doesn’t start from the last message, but the last “group” of messages.
```csharp
var offsetTypeLast = new OffsetTypeLast();
```
- Next: it takes messages published after the consumer connection.
```csharp
var offsetTypeNext = new OffsetTypeNext()
```
- Offset: it takes messages starting from the message with id equal to the passed value. If the value is less than the first message of the stream, it starts from the first (i.e. if you pass 0, but the stream starts from 10, it starts from 10). If the message with the id hasn’t yet been published it waits until this publishingId is reached.
```csharp
ulong iWantToStartFromPubId = 10;
var offsetTypeOffset = new OffsetTypeOffset(iWantToStartFromPubId);
```
- Timestamp: it takes messages starting from the first message with timestamp bigger than the one passed
```csharp
var anHourAgo = (long)DateTime.UtcNow.AddHours(-1).Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
var offsetTypeTimestamp = new OffsetTypeTimestamp(anHourAgo);
```
### Track Offset

The server can store the current delivered offset given a consumer with `StoreOffset` in this way:
```csharp
var messagesConsumed = 0;
var consumer = await system.CreateConsumer(
    new ConsumerConfig
    {
        Reference = "my_consumer",
        Stream = stream,
        MessageHandler = async (consumer, ctx, message) =>
        {
            if (++messagesConsumed % 1000 == 0)
            {
                await consumer.StoreOffset(ctx.Offset);
            }
```

Note: </b> 
**Avoid** to store the offset for each single message, it can reduce the performances.

It is possible to retrieve the offset with `QueryOffset`:

```csharp
var trackedOffset = await system.QueryOffset("my_consumer", stream);
var consumer = await system.CreateConsumer(
    new ConsumerConfig
    {
        Reference = "my_consumer",
        Stream = stream,
        OffsetSpec = new OffsetTypeOffset(trackedOffset),    
```

OBS. if don't have stored an offset for the consumer's reference on the stream you get an OffsetNotFoundException exception.

### Handle Close
Producers/Consumers raise and event when the client is disconnected:

```csharp
 new ProducerConfig/ConsumerConfig
 {
  ConnectionClosedHandler = s =>
   {
    Console.WriteLine($"Connection Closed: {s}");
    return Task.CompletedTask;
   },
```

### Handle Metadata Update
Stream metadata update is raised when the stream topology changes or the stream is deleted. 
You can use `MetadataHandler` to handle it:
```csharp
 new ProducerConfig/ConsumerConfig
 {
  MetadataHandler = update =>
   {
   ......  
   },
 }
```
### Reliable 
 - Reliable Producer
 - Reliable Consumer (not ready yet)

### Reliable Producer
Reliable Producer is a smart layer built up of the standard `Producer`. </b>   
The idea is to leave the user decides what to use, the standard or reliable producer. </b>

The main features are:
- Provide publishingID automatically
- Auto-Reconnect in case of disconnection
- Trace sent and received messages
- Invalidate messages
- Handle the metadata Update

#### Provide publishingID automatically
Reliable Producer retrieves the last publishingID given the producer name. </b>
Zero(0) is the default value in case there is no a publishingID.

#### Auto-Reconnect
Reliable Producer restores the TCP connection in case the Producer is disconnected for some reason.
During the reconnection it continues to store the messages in a local-list.
The user will receive back the confirmed or un-confirmed messages.

#### Trace sent and received messages
Reliable Producer keeps in memory each sent message and remove from the memory when the message is confirmed or goes in timout.
`ConfirmationHandler` receives the messages  back with the status.
`confirmation.Status` can have different values, but in general `ConfirmationStatus.Confirmed` means the messages
is stored on the server other status means that there was a problem with the message/messages.
```csharp
ConfirmationHandler = confirmation =>
{                    
    if (confirmation.Status == ConfirmationStatus.Confirmed)
    {

        // OK
    }
    else
    {
        // Some problem
    }
}
```
#### Invalidate messages
If the client doesn't receive a confirmation within 2 seconds Reliable Producer removes the message from the internal messages cache.
The user will receive `ConfirmationStatus.TimeoutError` in the `ConfirmationHandler`.

#### Handle the metadata Update
If the streams changes the topology (ex:Stream deleted or add/remove follower), the client receives an `MetadataUpdate` event.
Reliable Producer detects the event and tries to reconnect the producer if the stream still exist else closes the producer.
#### Send API
Reliable Producer implements two `send(..)`
- `Send(Message message)` // standard
- `Send(List<Message> messages, CompressionType compressionType)` //sub-batching with compression

### Reconnection Strategy 
By default Reliable Producer uses an `BackOffReconnectStrategy` to reconnect the client.
You can customize the behaviour implementing the `IReconnectStrategy` interface:
```csharp
void WhenDisconnected(out bool reconnect);
void WhenConnected();
```
with `reconnect` you can decide when reconnect the producer.

You can use it:
```csharp
var p = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
{
...
ReconnectStrategy = MyReconnectStrategy
```

#### Examples
See the directory [Examples/Reliable](./Examples/Reliable)


### Build from source
Build:
```shell
make build
```

Test:
```shell
make test
```

Run test in docker:
```shell
make run-test-in-docker
```

### Project Status

The client is work in progress. The API(s) could change