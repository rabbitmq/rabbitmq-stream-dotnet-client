// See https://aka.ms/new-console-template for more information

using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");


var rClient = RClient.Start(new RClient.Config()
{
    ProducersPerConnection = 3, 
    ConsumersPerConnection = 3,
    // Host = "34.77.246.214",
    Port = 5552,
    // LoadBalancer = true,
    SuperStream = true,
    Streams = 1,
    Producers = 2,
    MessagesPerProducer = 5_000_000,
    Consumers = 1,
    // Username = "test",
    // Password = "test"
});

await rClient.ConfigureAwait(false);
