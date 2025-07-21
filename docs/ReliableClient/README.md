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

You can customize the settings by using the [
`appsettings.json`](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/blob/f3b131cf6688843f2d91854a9d35ebc91be54090/docs/ReliableClient/appsettings.json)
file.
