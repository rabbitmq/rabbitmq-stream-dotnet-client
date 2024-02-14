// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.Configuration;
using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");
const string FileName = "appsettings.json";

var fs = File.OpenRead(FileName);


var section = new ConfigurationBuilder()
    .AddJsonFile(FileName)
    .Build()
    .GetSection("RabbitMQ");
fs.Dispose();


var rClient = BestPracticesClient.Start(new BestPracticesClient.Config()
{
    // set the ProducersPerConnection. This is the number of producers that will be created for each connection.
    // a low value can improve the throughput of the producer since the connection is shared between the producers.
    // a high value can reduce the resource usage of the producer since the connection is shared between the producers.
    ProducersPerConnection = section.GetSection("ProducersPerConnection").Get<byte>(),
    // Same rules as ProducersPerConnection but for the consumers.
    // Note that if a consumer is slow can impact the other consumers on the same connection.
    // There is a small internal buffer that can help to mitigate this issue.
    // but if the consumer is too slow, the buffer will be full and the other consumers will be impacted.
    ConsumersPerConnection = section.GetSection("ConsumersPerConnection").Get<byte>(),
    Host = section.GetSection("Host").Get<string>(),
    Port = section.GetSection("Port").Get<int>(),
    
    LoadBalancer = section.GetSection("LoadBalancer").Get<bool>(),
    
    // Enable the SuperStream stream feature.
    SuperStream = section.GetSection("SuperStream").Get<bool>(),
    
    // The number of streams that will be created. in case of super stream, this is the number of the partitions.
    Streams = section.GetSection("Streams").Get<int>(),
    // The number of producers that will be created for each stream.
    Producers = section.GetSection("Producers").Get<int>(),
    
    // The number of messages that will be sent by each producer.
    MessagesPerProducer = section.GetSection("MessagesPerProducer").Get<int>(),
    Consumers = section.GetSection("Consumers").Get<int>(),
    Username = section.GetSection("Username").Get<string>(),
    Password = section.GetSection("Password").Get<string>(),
    // The delay between each message sent by the producer.
    DelayDuringSendMs = section.GetSection("DelayDuringSendMs").Get<int>(),
    StreamName = section.GetSection("StreamName").Get<string>(),
});

await rClient.ConfigureAwait(false);
