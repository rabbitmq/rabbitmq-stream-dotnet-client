// See https://aka.ms/new-console-template for more information

using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");


var rClient = RClient.Start(new RClient.Config()
{
    ProducersPerConnection = 2, 
    ConsumersPerConnection = 100,
    Host = "localhost",
    Port = 5552,
    LoadBalancer = true,
    SuperStream = false,
    Streams = 10,
    Producers = 4,
    MessagesPerProducer = 50_000_000,
    Consumers = 4
    // Username = "test",
    // Password = "test"
});

await rClient.ConfigureAwait(false);
