
<h1 style="text-align:center;">RabbitMQ client for the stream protocol</h1>

---
<div style="text-align:center;">

[![Build Status](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions/workflows/main.yaml/badge.svg)](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
[![codecov](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client/branch/main/graph/badge.svg?token=OIA04ZQD79)](https://codecov.io/gh/rabbitmq/rabbitmq-stream-dotnet-client)
</div>

# Table of Contents

---

- [Overview](#overview)
- [Update to v1.0.0-rc.5 to >v1.0.0-rc.6](#update-to-rc6)
- [Installing via NuGet](#installing-via-nuget)
- [Getting started](#getting-started)
    - [Main Concepts](#main-concepts)
    - [Usage](#usage)
        - [Connect](#connect)
        - [Multi Host](#multi-host)
        - [TLS](#tls)
        - [Load Balancer](#load-balancer)
    - [Manage Streams](#manage-streams)
    - [Producer](#producer)
      - [Publish Messages](#publish-messages)
    - [Consumer](#consumer)
      - [Offset Types](#offset-types)
      - [Track Offset](#track-offset)
      - [Single Active Consumer](#single-active-consumer)
    - [Heartbeat](#heartbeat)
    - [Raw Clients](#raw)
      - [Raw Producer](#raw-producer)
          - [Deduplication](#Deduplication)
      - [Raw Consumer](#raw-consumer)
      - [Handle Metadata Update](#handle-metadata-update)
      - [Handle Close](#handle-close)
- [Build from source](#build-from-source)
- [Project Status](#project-status)
- [Release Process](#release-process)

## Overview

Dotnet client for [RabbitMQ Stream Queues](https://www.rabbitmq.com/stream.html)

## Update to rc6
We introduced a few breaking changes. 
Read the [release notes](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/releases/tag/v1.0.0-rc.6)
to update the client.

## Installing via NuGet

The client is [distributed via NuGet](https://www.nuget.org/packages/RabbitMQ.Stream.Client/).

## Getting started

A rapid getting started

```csharp
public static async Task Start()
    {
        var config = new StreamSystemConfig
        {
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/"
        };
        // Connect to the broker and create the system object
        // the entry point for the client.
        // Create it once and reuse it.
        var system = await StreamSystem.Create(config);

        const string stream = "my_first_stream";

        // Create the stream. It is important to put some retention policy 
        // in this case is 200000 bytes.
        await system.CreateStream(new StreamSpec(stream)
        {
            MaxLengthBytes = 200000,
        });

        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                Reference = Guid.NewGuid().ToString(),


                // Receive the confirmation of the messages sent
                ConfirmationHandler = confirmation =>
                {
                    switch (confirmation.Status)
                    {
                        // ConfirmationStatus.Confirmed: The message was successfully sent
                        case ConfirmationStatus.Confirmed:
                            Console.WriteLine($"Message {confirmation.PublishingId} confirmed");
                            break;
                        // There is an error during the sending of the message
                        case ConfirmationStatus.WaitForConfirmation:
                        case ConfirmationStatus.ClientTimeoutError
                            : // The client didn't receive the confirmation in time. 
                        // but it doesn't mean that the message was not sent
                        // maybe the broker needs more time to confirm the message
                        // see TimeoutMessageAfter in the ProducerConfig
                        case ConfirmationStatus.StreamNotAvailable:
                        case ConfirmationStatus.InternalError:
                        case ConfirmationStatus.AccessRefused:
                        case ConfirmationStatus.PreconditionFailed:
                        case ConfirmationStatus.PublisherDoesNotExist:
                        case ConfirmationStatus.UndefinedError:
                        default:
                            Console.WriteLine(
                                $"Message  {confirmation.PublishingId} not confirmed. Error {confirmation.Status}");
                            break;
                    }

                    return Task.CompletedTask;
                }
            });

        // Publish the messages
        for (var i = 0; i < 100; i++)
        {
            var message = new Message(Encoding.UTF8.GetBytes($"hello {i}"));
            await producer.Send(message);
        }

// not mandatory. Just to show the confirmation
        Thread.Sleep(TimeSpan.FromSeconds(1));

// Create a consumer
        var consumer = await Consumer.Create(
            new ConsumerConfig(system, stream)
            {
                Reference = "my_consumer",
                // Consume the stream from the beginning 
                // See also other OffsetSpec 
                OffsetSpec = new OffsetTypeFirst(),
                // Receive the messages
                MessageHandler = async (sourceStream, consumer, ctx, message) =>
                {
                    Console.WriteLine(
                        $"message: coming from {sourceStream} data: {Encoding.Default.GetString(message.Data.Contents.ToArray())} - consumed");
                    await Task.CompletedTask;
                }
            });
        Console.WriteLine($"Press to stop");
        Console.ReadLine();

        await producer.Close();
        await consumer.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }

```
### Main Concepts

The client is based on the following concepts:

| Class  | Description  | How to get  | Note | Documentation| 
|---|---|---|-----------------|--------------|
|  `Producer`    | Hight Level producer. | `Producer.Create(new ProducerConfig(system, stream))`  | -Handle `RawProducer` and  `RawSuperStreamProducer` <br> - Auto reconnect <br> - Auto publishing id <br> - Handle Metadata update <br> - Confirm Messages with errors (timeout..etc) | [Doc Producer](#producer) |
|  `Consumer`    | Hight Level Consumer. | `Consumer.Create(new ConsumerConfig(system, stream))`  | -Handle `RawConsumer` and  `RawSuperStreamConsumer` <br> - Auto reconnect <br> - Handle Metadata update <br>                                                                         | [Doc Consumer](#consumer) |
|  `RawProducer` | Low-Level producer   |  `system.CreateRawProducer(new RawProducerConfig("stream")` || [Doc Raw Producer](#raw-producer)                                                                                                                                                    |
|  `RawSuperStreamProducer` | Low Level Super Stream Producer   |  `system.RawCreateSuperStreamProducer(new RawSuperStreamProducerConfig("superStream")` ||                                                                                                                                                                                      |
|  `RawConsumer` | Low Level consumer   |  `system.CreateRawConsumer(newRawConsumerConfig("stream")` || [Doc Raw Consumer](#raw-consumer)                                                                                                                                                    |
|  `RawSuperStreamConsumer` | Low Level Super Stream Consumer   |  `system.RawCreateSuperStreamConsumer(new RawSuperStreamConsumerConfig("superStream")` ||

You should use `Producer` and `Consumer` classes unless you need to handle the low-level details.
## Usage

---

### Connect

`StreamSystem` is the entry point for the client. It is the connection to the broker.
Just create it once and reuse it.

`StreamSystem` is responsible to handle the `stream-locator` tcp connection. That is the main connection to lookup the resources as leader connection, query the metadata etc.. .

```csharp

var config = new StreamSystemConfig
{
    UserName = "myuser",
    Password = "mypassword",
    VirtualHost = "myhost",
    Endpoints = new List<EndPoint> {new IPEndPoint(IPAddress.Parse("<<brokerip>>"), 5552)},
};

var system = await StreamSystem.Create(config);
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
var system = await StreamSystem.Create(config);
```

### TLS

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
};
var system = await StreamSystem.Create(config);
```

### TLS with client certificate

```csharp
var config = new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    Ssl = new SslOption()
    {
        Enabled = true,
        CertPath = "C:\\path\\to\\cert\\file.pfx",
        CertPassphrase = "Password"
    },
};
var system = await StreamSystem.Create(config);
```

### Load Balancer

See https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#with-a-load-balancer for more information

```csharp
var lbAddressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("<<loadBalancerIP>>"), 5552));
var config = new StreamSystemConfig
{
    UserName = "guest",
    Password = "guest",
    VirtualHost = "/",
    AddressResolver = lbAddressResolver,
    Endpoints = new List<EndPoint> {addressResolver.EndPoint},
};  
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
This is also used for truncate the stream: when a stream reaches his maximum size, an entire segment file is deleted.
For this reason  `MaxLengthBytes` (the max dimension of the entire stream) is usually significantly higher
than `MaxSegmentSizeBytes` (the max dimension of a single segment file).
RabbitMQ enforces the retention policy when the current segment has reached its maximum size and is closed in favor of a
new one.

```csharp
await system.CreateStream(new StreamSpec(stream)
{
    MaxLengthBytes = 20_000,
    MaxSegmentSizeBytes = 1000
});
```

### Producer

A Producer instance is created from the `Producer`.

```csharp
  var producer = await Producer.Create(  
  new ProducerConfig(system, stream) {
  
  });
```

Consider a Producer instance like a long-lived object, do not create one to send just one message.

| Parameter               | Description                            | Default                        |
|-------------------------|----------------------------------------|--------------------------------|
| StreamSystem                  | the stream system where to connect              | No default, mandatory setting. | 
| Stream                  | The stream to publish to.              | No default, mandatory setting. | 
| Reference               | The logical name of the producer.      | null (no deduplication)        | 
| ClientProvidedName      | Set the TCP Client Name                | `dotnet-stream-producer`       | 
| ConfirmationHandler          | Handler with confirmed messages        | It is an event                 |
| TimeoutMessageAfter          | TimeoutMessageAfter is the time after which a message is considered as timed out        | TimeSpan.FromSeconds(3)                 |

Producer with a reference name stores the sequence id on the server.
It is possible to retrieve the id using `producer.GetLastPublishingId()`
or more generic `system.QuerySequence("reference", "my_stream")`.


`Producer` handles the following features automatically:

- Provide incremental publishingId 
- Auto-Reconnect in case of disconnection
- Trace sent and received messages
- Invalidate messages
- [Handle the metadata Update](#handle-metadata-update)

#### Provide publishingId automatically

`Producer` retrieves the last publishingID given the producer name.

Zero(0) is the default value in case there is no publishingId for given producer reference.

#### Auto-Reconnect

`Producer` restores the TCP connection in case the `Producer` is disconnected for some reason.
During the reconnection it continues to store the messages in a local-list.
The user will receive back the confirmed or un-confirmed messages.
See [Reconnection Strategy](#reconnection-strategy)

#### Trace sent and received messages

`Producer` keeps in memory each sent message and removes it from the memory when the message is confirmed or
times out.
`ConfirmationHandler` receives the messages back with the status.
`confirmation.Status` can have different values, but in general `ConfirmationStatus.Confirmed` means the messages
is stored on the server. Other statuses mean that there was a problem with the message/messages under given publishing
id.

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

#### Currently defined confirmation statuses

| Status                | Description                                                                                                  | Source |
|-----------------------|--------------------------------------------------------------------------------------------------------------|--------|
| Confirmed             | Message has been confirmed by the server and written to disk.                                                | Server |
| ClientTimeoutError    | Client gave up waiting for the message (read more [here](#invalidate-messages)).                             | Client |
| StreamNotAvailable    | Stream was deleted or otherwise become unavailable.                                                          | Server |
| InternalError         |                                                                                                              | Server |
| AccessRefused         | Provided credentials are invalid or you lack permissions for specific vhost/etc.                             | Server |
| PreconditionFailed    | Catch-all for validation on server (eg. requested to create stream with different parameters but same name). | Server |
| PublisherDoesNotExist |                                                                                                              | Server |
| UndefinedError        | Catch-all for any new status that is not yet handled in the library.                                         | Server |

#### Invalidate messages

If the client doesn't receive a confirmation within configured timeout (3 seconds by default), Reliable Producer removes
the message from the internal messages cache.
The user will receive `ConfirmationStatus.ClientTimeoutError` in the `ConfirmationHandler`.

### Publish Messages

#### Standard publish

```csharp
    var message = new Message(Encoding.UTF8.GetBytes("hello"));
    await producer.Send(message);
```

#### Batch publish

Batch send is a synchronous operation.
It allows to pre-aggregate messages and send them in a single synchronous call.

```csharp
var messages = new List<Message>();
for (ulong i = 0; i < 30; i++)
{
    messages.Add(new Message(Encoding.UTF8.GetBytes($"batch {i}")));
}
await producer.Send(messages);
messages.Clear();
```

In most cases, the standard `Send` is easier and works in most of the cases.

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
await producer.Send(subEntryMessages, CompressionType.Gzip);
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

### Publish SuperStream
See: https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams for more details.

```csharp
 var producer = await Producer.Create(new ProducerConfig(system, "super_stream")
        {
            SuperStreamConfig = new SuperStreamConfig()
            {
                Routing = message1 => message1.Properties.MessageId.ToString()
            }
        }
        );
```

`SuperStreamConfig` is mandatory to enable the super stream feature.
`Routing` is a function that extracts the routing key from the message. By default it uses a `HashRoutingMurmurStrategy` strategy.

See `Tests.SuperStreamProducerTests.ValidateHashRoutingStrategy` for more examples.


### Consumer

Define a consumer:

```csharp
var consumer = await Consumer.Create(
    new ConsumerConfig(system,stream)
    {
        Reference = "my_consumer",
        MessageHandler = async (sourceStream, consumer, ctx, message) =>
        {
            Console.WriteLine(
                $"message: {Encoding.Default.GetString(message.Data.Contents.ToArray())}");
            await Task.CompletedTask;
        }
});
```

`Consumer` handle the following feature automatically:

- Auto-Reconnect in case of disconnection
- Auto restart consuming from the last offset
- [Handle the metadata Update](#reliable-handle-metadata-update)

#### Auto-Reconnect

`Consumer` restores the TCP connection in case the Producer is disconnected for some reason.
`Consumer` will restart consuming from the last offset stored.
See [Reconnection Strategy](#reconnection-strategy)


### Offset Types

There are five types of Offset and they can be set by the `ConsumerConfig.OffsetSpec` property that must be passed to
the Consumer constructor, in the example we use `OffsetTypeFirst`:

```csharp
...        
 OffsetSpec = new OffsetTypeFirst(),
 ...      
```

The five types are:

- First: it takes messages from the first message of the stream.

```csharp
var offsetTypeFirst = new OffsetTypeFirst();
```

- Last: it takes messages from the last chunk of the stream, i.e. it doesn’t start from the last message, but the last
  “group” of messages.

```csharp
var offsetTypeLast = new OffsetTypeLast();
```

- Next: it takes messages published after the consumer connection.

```csharp
var offsetTypeNext = new OffsetTypeNext()
```

- Offset: it takes messages starting from the message with id equal to the passed value. If the value is less than the
  first message of the stream, it starts from the first (i.e. if you pass 0, but the stream starts from 10, it starts
  from 10). If the message with the id hasn’t yet been published it waits until this publishingId is reached.

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
....        
         MessageHandler = async (sourceStream,consumer, ctx, message) =>
        {
            if (++messagesConsumed % 1000 == 0)
            {
                await consumer.StoreOffset(ctx.Offset);
            }
```

Note: **Avoid** storing the offset for every single message, it can reduce performance.

It is possible to retrieve the offset with `QueryOffset`:

```csharp
var trackedOffset = await system.QueryOffset("my_consumer", stream);
...
    new ConsumerConfig(system,stream)
    {
        Reference = "my_consumer",
        OffsetSpec = new OffsetTypeOffset(trackedOffset),    
```

Note: if you try to store an offset that doesn't exist yet for the consumer's reference on the stream you get will get
an `OffsetNotFoundException` exception.

### Single Active Consumer

Use the `ConsumerConfig#IsSingleActiveConsumer()` method to enable the feature:

Enabling single active consumer

```csharp
     new ConsumerConfig(system,stream)
    {
        Reference = "application-1", // Set the consumer name (mandatory to enable single active consumer)
        IsSingleActiveConsumer = true, // Enable single active consumer
        OffsetSpec = new OffsetTypeFirst(),
    ...
    });
```

With the configuration above, the consumer will take part in the `application-1` group on the `my-stream` stream.
If the consumer instance is the first in a group, it will get messages as soon as there are some available.
If it is not the first in the group, it will remain idle until it is its turn to be active
(likely when all the instances registered before it are gone).

By default the Single Active Consumer start consuming form the `OffsetSpec` but you can override it with the
`ConsumerUpdateListener` event.

`ConsumerUpdateListener` returns an `OffsetType` that will be used to start consuming from.

For example, if you want to start from the last tracked message can do it like this:

```csharp
    new ConsumerConfig(system,stream)
    {
        Reference = "application-1",
        Stream = "my-stream",
        IsSingleActiveConsumer = true,
        // When the consumer is actived it will start from the last tracked offset
        ConsumerUpdateListener = async (reference, stream, isActive) =>
        {
            var trackedOffset = await system.QueryOffset(reference, stream);
            return new OffsetTypeOffset(trackedOffset);
        };
```

### Reconnection Strategy

By default Reliable Producer/Consumer uses an `BackOffReconnectStrategy` to reconnect the client.
You can customize the behaviour implementing the `IReconnectStrategy` interface:

```csharp
ValueTask<bool> WhenDisconnected(string connectionInfo);
ValueTask WhenConnected(string connectionInfo);
```

If `WhenDisconnected` return is `true` Producer/Consumer will be reconnected else closed.
`connectionInfo` add information about the connection.

You can use it:

```csharp
var p = await Producer.Create(new ReliableProducerConfig(system, stream)
{
    ...
        ReconnectStrategy = MyReconnectStrategy
    ...
});
```

### Handle metadata update

If the streams changes the topology (ex:Stream deleted or add/remove follower), the client receives an `MetadataUpdate`
event.
Reliable Producer detects the event and tries to reconnect the producer if the stream still exist else closes the
producer/consumer.


### Heartbeat

It is possible to configure the heartbeat using:

```csharp
 var config = new StreamSystemConfig()
{
     Heartbeat = TimeSpan.FromSeconds(30),
}
```

- `60` (`TimeSpan.FromSeconds(60)`) seconds is the default value
- `0` (`TimeSpan.FromSeconds(0)`) will advise server to disable heartbeat

Heartbeat value shouldn't be too low.

### Raw 

- Raw Producer
- Raw Consumer


### Raw Producer

`RawProducer` is the low level producer provided `system` 

### Deduplication

[See here for more details](https://rabbitmq.github.io/rabbitmq-stream-java-client/snapshot/htmlsingle/#outbound-message-deduplication)
Set a producer reference to enable the deduplication:

```csharp
var producer = await system.CreateRawProducer(
    new ProducerConfig("my_stream")
    {
        Reference = "my_producer",
        
    });
```

then:

```csharp
var publishingId = 0;
var message = new Message(Encoding.UTF8.GetBytes($"my deduplicate message {i}"));
await producer.Send(publishingId, message);
```

Note: at the moment is only available for the `RawProducer`

### Raw Consumer

`RawConsumer` is the low level consumer provided `system` 

```csharp
  var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {..}
            ); 
```


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

## Build from source

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

## Project Status

The client is work in progress. The API(s) could change prior to version `1.0.0`

## Release Process

* Ensure builds are green: [link](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
* Tag the `main` branch using your GPG key:
    ```
    git tag -a -s -u GPG_KEY_ID -m 'rabbitmq-stream-dotnet-client v1.0.0' 'v1.0.0' && git push && git push --tags
    ```
* Ensure the build for the tag passes: [link](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/actions)
* Create the new release on GitHub, which triggers a build and publish to NuGet: [link](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/releases)
* Check for the new version on NuGet: [link](https://www.nuget.org/packages/RabbitMQ.Stream.Client)
* Best practice is to download the new package and inspect the contents using [NuGetPackageExplorer](https://github.com/NuGetPackageExplorer/NuGetPackageExplorer)
* Announce the new release on the mailing list: [link](https://groups.google.com/g/rabbitmq-users)
