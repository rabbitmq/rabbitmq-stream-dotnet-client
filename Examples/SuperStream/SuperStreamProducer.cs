// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStream;

public class SuperStreamProducer
{
    public static async Task Start()
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var logger = loggerFactory.CreateLogger<Producer>();
        var loggerMain = loggerFactory.CreateLogger<SuperStreamProducer>();

        loggerMain.LogInformation("Starting SuperStream Producer");
        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        loggerMain.LogInformation("Super Stream Producer connected to RabbitMQ");


        // We define a Producer with the SuperStream name (that is the Exchange name)
        var producer = await Producer.Create(new ProducerConfig(system, Costants.StreamName)
        {
            SuperStreamConfig = new SuperStreamConfig()
            {
                // The super stream is enable and we define the routing hashing algorithm
                Routing = msg => msg.Properties.MessageId.ToString()
            }
        }, logger).ConfigureAwait(false);
        const int NumberOfMessages = 1_000_000;
        for (var i = 0; i < NumberOfMessages; i++)
        {
            var message = new Message(Encoding.Default.GetBytes($"hello{i}"))
            {
                Properties = new Properties() {MessageId = $"hello{i}"}
            };
            await producer.Send(message).ConfigureAwait(false);
            loggerMain.LogInformation("Super Stream Producer sent {I} messages to {StreamName}", i, Costants.StreamName);
            Thread.Sleep(TimeSpan.FromMilliseconds(1000));
        }
    }
}
