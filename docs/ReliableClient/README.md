Best practices for reliable client
----------------------------------

This is an example of how to use the client in a reliable way. By following these best practices, you can ensure
that your application will be able to recover from network failures and other issues.

The producer part is the most important: you need to block the sending of messages until the connection is established.
This is done by using the [
`ManualResetEvent`](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/f3b131cf6688843f2d91854a9d35ebc91be54090/docs/ReliableClient/BestPracticesClient.cs#L251)
class.

You'll also need a list to store any messages that were not sent because the connection was not yet established.

Use the [
`Identifier`](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/f3b131cf6688843f2d91854a9d35ebc91be54090/docs/ReliableClient/BestPracticesClient.cs#L193-L254)
property to identify the producer or consumer in the logs.

Focus on how to handle the entity-level [
`StatusChanged`](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/f3b131cf6688843f2d91854a9d35ebc91be54090/docs/ReliableClient/BestPracticesClient.cs#L279-L309)
events.

## Configuration

All the settings are provided via environment variables (see `Program.cs`). There is no configuration file.

| Environment variable        | Default            | Description                                                         |
|------------------------------|---------------------|-----------------------------------------------------------------------|
| `HOST`                       | `localhost`         | RabbitMQ node hostname or IP address                                  |
| `PORT`                        | `5552`               | RabbitMQ Streaming port                                                |
| `USERNAME`                   | `guest`              | Username                                                               |
| `PASSWORD`                   | `guest`              | Password                                                               |
| `LOAD_BALANCER`               | `false`              | Enable when connecting through a load balancer                        |
| `STREAM_NAME`                 | `DotNetClientTest`   | Base stream name (or super stream name)                               |
| `SUPER_STREAM`                 | `false`              | Enable the super stream feature                                       |
| `STREAMS`                      | `1`                  | Number of streams (or partitions, if `SUPER_STREAM` is enabled)        |
| `PRODUCERS`                    | `9`                  | Number of producers created per stream                                |
| `PRODUCERS_PER_CONNECTION`      | `7`                  | Number of producers sharing the same connection                        |
| `MESSAGES_PER_PRODUCER`         | `5000000`            | Number of messages sent by each producer                              |
| `DELAY_DURING_SEND_MS`          | `0`                  | Delay, in milliseconds, between each message sent by a producer        |
| `ENABLE_RESENDING`              | `false`              | Resend messages that were not confirmed                                |
| `CONSUMERS`                     | `9`                  | Number of consumers created per stream                                |
| `CONSUMERS_PER_CONNECTION`       | `8`                  | Number of consumers sharing the same connection                        |

## Running with Docker

Build the image from the repository root (the build needs the client source in `RabbitMQ.Stream.Client`):

```shell
docker build -f docs/ReliableClient/Dockerfile -t rabbitmq-stream-reliable-client .
```

Run it, overriding only the environment variables you need:

```shell
docker run --rm \
  -e HOST=my-rabbitmq \
  -e USERNAME=guest \
  -e PASSWORD=guest \
  rabbitmq-stream-reliable-client
```

When the container is started without a TTY (e.g. `docker run -d ...`), the client skips the "press any key"
prompts and keeps running until it receives `SIGINT`/`SIGTERM` (e.g. `docker stop`).
