// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries..

using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Filter;

public class FilterSuperStreamConsumer
{
    public static async Task Start(string streamName)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Debug);
        });

        var logger = loggerFactory.CreateLogger<Consumer>();
        var loggerMain = loggerFactory.CreateLogger<FilterSuperStreamConsumer>();


        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        loggerMain.LogInformation("FilterSuperStreamConsumer connected to RabbitMQ. StreamName {StreamName}",
            streamName);


        // tag::consumer-filter[]

        var consumedMessages = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, streamName)
        {
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,

            // This is mandatory for enabling the filter
            Filter = new ConsumerFilter()
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
        }, logger).ConfigureAwait(false);

        await Task.Delay(2000).ConfigureAwait(false);
        await consumer.Close().ConfigureAwait(false);
        await system.Close().ConfigureAwait(false);
    }
}
