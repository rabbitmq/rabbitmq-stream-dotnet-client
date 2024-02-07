// See https://aka.ms/new-console-template for more information

using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");


var rClient = RClient.Start(new RClient.Config()
{
    ProducersPerConnection = 100, 
    ConsumersPerConnection = 100,
    Host = "node0",
    Port = 5552,
    LoadBalancer = true,
    SuperStream = false,
    Streams = 3,
    Producers = 10,
    MessagesPerProducer = 50_000_000,
    Consumers = 10,
    Username = "test",
    Password = "test"
});

await rClient.ConfigureAwait(false);
