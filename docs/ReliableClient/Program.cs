// See https://aka.ms/new-console-template for more information

using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");


var rClient = RClient.Start(new RClient.Config()
{
    ProducersPerConnection = 2, 
    ConsumersPerConnection = 2,
    Host = "Node0",
    Port = 5553,
    LoadBalancer = true,
    SuperStream = true,
    Streams = 3,
    Producers = 4,
    MessagesPerProducer = 50_000_000,
    Consumers = 4
    // Username = "test",
    // Password = "test"
});

await rClient.ConfigureAwait(false);
