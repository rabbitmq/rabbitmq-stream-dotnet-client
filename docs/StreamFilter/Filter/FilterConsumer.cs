// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Filter;

public class FilterConsumer
{
    public static async Task Start(string streamName)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var logger = loggerFactory.CreateLogger<Consumer>();
        var loggerMain = loggerFactory.CreateLogger<FilterConsumer>();


        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        await system.CreateStream(new StreamSpec(streamName)).ConfigureAwait(false);
        loggerMain.LogInformation("FilterConsumer connected to RabbitMQ. StreamName {StreamName}", streamName);


        // tag::consumer-filter[]

        var consumedMessages = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, streamName)
        {
            OffsetSpec = new OffsetTypeFirst(),

            // This is mandatory for enabling the filter
            Filter = new RabbitMQ.Stream.Client.ConsumerFilter()
            {
                Values = new List<string>() {"Alabama"},
                PostFilter = message => message.ApplicationProperties["state"].Equals("Alabama"), // <1>
                MatchUnfiltered = true // <2>
            },
            MessageHandler = (_, _, _, message) =>
            {
                logger.LogInformation("Received message with state {State} - consumed {Consumed}",
                    message.ApplicationProperties["state"], ++consumedMessages);
                return Task.CompletedTask;
            }
            // end::consumer-filter[]
        }).ConfigureAwait(false);
        
        await Task.Delay(2000).ConfigureAwait(false);
        await consumer.Close().ConfigureAwait(false);
        await system.Close().ConfigureAwait(false);
    }
}
