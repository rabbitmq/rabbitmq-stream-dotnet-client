// See https://aka.ms/new-console-template for more information

using System;
using ReliableClient;

Console.WriteLine("Starting RabbitMQ Streaming Client");

static string GetEnvString(string name, string defaultValue) =>
    Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : defaultValue;

static int GetEnvInt(string name, int defaultValue) =>
    int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : defaultValue;

static byte GetEnvByte(string name, byte defaultValue) =>
    byte.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : defaultValue;

static bool GetEnvBool(string name, bool defaultValue) =>
    bool.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : defaultValue;

var rClient = BestPracticesClient.Start(new BestPracticesClient.Config()
{
    Host = GetEnvString("HOST", "localhost"),
    Port = GetEnvInt("PORT", 5552),
    Username = GetEnvString("USERNAME", "guest"),
    Password = GetEnvString("PASSWORD", "guest"),
    StreamName = GetEnvString("STREAM_NAME", "DotNetClientTest"),
    LoadBalancer = GetEnvBool("LOAD_BALANCER", false),

    // Enable the SuperStream stream feature.
    SuperStream = GetEnvBool("SUPER_STREAM", false),

    // The number of streams that will be created. in case of super stream, this is the number of the partitions.
    Streams = GetEnvInt("STREAMS", 1),
    // The number of producers that will be created for each stream.
    Producers = GetEnvInt("PRODUCERS", 9),
    // set the ProducersPerConnection. This is the number of producers that will be created for each connection.
    // a low value can improve the throughput of the producer since the connection is shared between the producers.
    // a high value can reduce the resource usage of the producer since the connection is shared between the producers.
    ProducersPerConnection = GetEnvByte("PRODUCERS_PER_CONNECTION", 7),

    // The number of messages that will be sent by each producer.
    MessagesPerProducer = GetEnvInt("MESSAGES_PER_PRODUCER", 5_000_000),
    Consumers = GetEnvInt("CONSUMERS", 9),
    // Same rules as ProducersPerConnection but for the consumers.
    // Note that if a consumer is slow can impact the other consumers on the same connection.
    // There is a small internal buffer that can help to mitigate this issue.
    // but if the consumer is too slow, the buffer will be full and the other consumers will be impacted.
    ConsumersPerConnection = GetEnvByte("CONSUMERS_PER_CONNECTION", 8),

    // The delay between each message sent by the producer.
    DelayDuringSendMs = GetEnvInt("DELAY_DURING_SEND_MS", 0),
    EnableResending = GetEnvBool("ENABLE_RESENDING", false),
    
    DeleteStreamsOnStart = GetEnvBool("DELETE_STREAMS_ON_START", true),
});

await rClient.ConfigureAwait(false);
