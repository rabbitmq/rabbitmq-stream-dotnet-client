// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using Microsoft.Extensions.Logging;

namespace SingleActiveConsumer;

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

public class SaCProducer
{
    public static async Task Start()
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var loggerProducer = loggerFactory.CreateLogger<Producer>();
        var loggerMain = loggerFactory.CreateLogger<StreamSystem>();


        var streamSystem = await StreamSystem.Create(new StreamSystemConfig(), loggerMain).ConfigureAwait(false);
        await streamSystem.CreateStream(new StreamSpec("my-sac-stream")).ConfigureAwait(false);
        var producer = await Producer.Create(new ProducerConfig(streamSystem, "my-sac-stream"), loggerProducer)
            .ConfigureAwait(false);
        for (var i = 0; i < 5000; i++)
        {
            var body = Encoding.UTF8.GetBytes($"Message #{i}");
            var message = new Message(body);
            await producer.Send(message).ConfigureAwait(false);
            Thread.Sleep(2000);
            loggerProducer.LogInformation($"Message {i} sent");
        }

        Console.WriteLine("Sending 50 messages to my-sac-stream");
        await producer.Close().ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
    }
}
