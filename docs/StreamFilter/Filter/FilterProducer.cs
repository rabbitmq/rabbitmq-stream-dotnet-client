// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace Filter;

public class FilterProducer
{
    public static async Task Start(string streamName)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var logger = loggerFactory.CreateLogger<Producer>();
        var loggerMain = loggerFactory.CreateLogger<FilterProducer>();


        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        await system.CreateStream(new StreamSpec(streamName)).ConfigureAwait(false);
        loggerMain.LogInformation("FilterProducer connected to RabbitMQ. StreamName {StreamName}", streamName);

        var producer = await Producer.Create(new ProducerConfig(system, streamName)
        {
            // tag::producer-filter[]

            // This is mandatory for enabling the filter
            FilterValue = message => message.ApplicationProperties["state"].ToString(), // <1>
            // end::producer-filter[]
        }).ConfigureAwait(false);

        var messagesSent = 0;

        // Send the first 200 messages with state "New York"
        // then we wait a bit to be sure that all the messages will go in a chuck
        for (var i = 0; i < 200; i++)
        {
            const string State = "New York";
            var message = new Message(Encoding.UTF8.GetBytes($"Message: {i}.  State: {State}"))
            {
                ApplicationProperties = new ApplicationProperties()
                {
                    ["state"] = State // <2>
                }
            };
            await producer.Send(message).ConfigureAwait(false);
            
            logger.LogInformation("Published message with state {State} messages sent {Sent}", message.ApplicationProperties["state"], ++messagesSent);
        }

        // Wait a bit to be sure that all the messages will go in a chuck
        await Task.Delay(2000).ConfigureAwait(false);

        // Send the second 200 messages with the Alabama state
        for (var i = 0; i < 200; i++)
        {
            const string State = "Alabama";
            var message = new Message(Encoding.UTF8.GetBytes($"Message: {i}.  State: {State}"))
            {
                ApplicationProperties = new ApplicationProperties()
                {
                    ["state"] = State
                }
            };
            await producer.Send(message).ConfigureAwait(false);
            logger.LogInformation("Published message with state {State} messages sent {Sent}", message.ApplicationProperties["state"], ++messagesSent);
        }

        await Task.Delay(1000).ConfigureAwait(false);
        await producer.Close().ConfigureAwait(false);
        await system.Close().ConfigureAwait(false);
    }
}
