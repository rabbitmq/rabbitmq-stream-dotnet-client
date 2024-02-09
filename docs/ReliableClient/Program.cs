// See https://aka.ms/new-console-template for more information

using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");


var rClient = RClient.Start(new RClient.Config()
{
    ProducersPerConnection = 100, 
    ConsumersPerConnection = 100,
    Host = "node0",
    Port = 5562,
    LoadBalancer = false,
    SuperStream = false,
    Streams = 3,
    Producers = 10,
    MessagesPerProducer = 10_000_000,
    Consumers = 10,
    // Username = "test",
    // Password = "test"
});

await rClient.ConfigureAwait(false);
