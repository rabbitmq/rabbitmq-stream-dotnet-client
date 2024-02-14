Best practices for reliable client
----------------------------------

This is an example of hot to use the client in a reliable way. By following these best practices, you can ensure that your application will be able to recover from network failures and other issues.

the producer part is the most important, you need to block the sending of messages until the connection is established. This is done by using the `ManualResetEvent` method.

you'd need also a list to store the messages that were not sent because the connection was not established.

Use the `Identify` method to identify the producer or consumer it the logs.

You should focus in how to deal with the entity `StatusChanged` events.

You can customize the setting using `appsetting.json` file.



