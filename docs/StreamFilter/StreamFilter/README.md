Server Side Filter Example
--------------------------

This example shows how to use the server side filter to filter the data on the server side.

### Running the Producer 

```
$  dotnet run --producer
```

The producer will send 400 messages to the server 200 with the filter value  equal to `"New York"` and 200 with the filter value equal to `"Alabama"`

Between each send the producer will wait 2 seconds to be sure that the messages will end up in different chunks.

### Running the Consumer

```
$  dotnet run --consumer
```

The consumer will connect with the filter "Alabama" and will receive 200 messages.
That is the second chunk of messages.

The `PostFilter` applies a client-side filter in case there are other kind of the messages inside the chunk.
         

As result the consumer will receive 200 messages with the filter value equal to `"Alabama"`.

Like:
```
....
      Received message with state Alabama - consumed 197
info: RabbitMQ.Stream.Client.Reliable.Consumer[0]
      Received message with state Alabama - consumed 198
info: RabbitMQ.Stream.Client.Reliable.Consumer[0]
      Received message with state Alabama - consumed 199
info: RabbitMQ.Stream.Client.Reliable.Consumer[0]
      Received message with state Alabama - consumed 200
```

You can play with the filter by changing the filter values and the post filter like:

```csharp
     Values = new List<string>() {"Alabama", "New York"},
     PostFilter = (msg) => message.ApplicationProperties["state"].Equals("Alabama") || message.ApplicationProperties["state"].Equals("New York")
```

To see the server side filter you can change the `PostFilter` to accept all the messages like:

```
 Values = new List<string>() {"Alabama", "New York"},
 PostFilter = (msg) => true
```

In this case the consumer will receive 400 messages.


```
 Values = new List<string>() {"Alabama"},
 PostFilter = (msg) => true
```

In this case the consumer will receive 200 messages.

```
 Values = new List<string>() {"New York"},
 PostFilter = (msg) => true
```

Note: In this example we forced the chunks to have a consistent messages with the same filter value. ("Alabama" or "New York")

In real world the chunks can have different messages with different filter values so the PostFilter is needed to filter the messages on the client side.
