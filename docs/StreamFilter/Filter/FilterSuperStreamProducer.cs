// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace Filter;

public class FilterSuperStreamProducer
{
    public static async Task Start(string streamName)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var logger = loggerFactory.CreateLogger<Producer>();
        var loggerMain = loggerFactory.CreateLogger<FilterSuperStreamProducer>();


        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config).ConfigureAwait(false);
        loggerMain.LogInformation("FilterSuperStreamProducer connected to RabbitMQ. StreamName {StreamName}", streamName);

        var producer = await Producer.Create(new ProducerConfig(system, streamName)
        {
            // tag::producer-filter[]
            SuperStreamConfig = new SuperStreamConfig()
            {
              Routing  = message => message.Properties.GroupId.ToString()
            },

            // This is mandatory for enabling the filter
            FilterValue = message => message.ApplicationProperties["state"].ToString(), // <1>
            // end::producer-filter[]
        }).ConfigureAwait(false);


        const int ToSend = 100;
        async Task SendTo(string state, string groupId)
        {
            var messages = new List<Message>();
            for (var i = 0; i < ToSend; i++)
            {
                var message = new Message(Encoding.UTF8.GetBytes($"Message: {i}.  State: {state}"))
                {
                    Properties = new Properties()
                    {
                        GroupId = groupId,
                    },
                    
                    ApplicationProperties = new ApplicationProperties()
                    {
                        ["state"] = state 
                    }
                };
                await producer.Send(message).ConfigureAwait(false);
                messages.Add(message);
            }

            await producer.Send(messages).ConfigureAwait(false);
        }
        
        
        // Send the first 200 messages with state "New York"
        // then we wait a bit to be sure that all the messages will go in a chuck
        await SendTo("New York", "NewYorkGroup").ConfigureAwait(false);
        loggerMain.LogInformation("Sent: {MessagesSent} - filter value: {FilerValue}", ToSend * 2, "New York");
        
        // Wait a bit to be sure that all the messages will go in a chuck
        await Task.Delay(2000).ConfigureAwait(false);

        // Send the second 200 messages with the Alabama state
        await SendTo("Alabama", "AlabamaGroup").ConfigureAwait(false);
        loggerMain.LogInformation("Sent: {MessagesSent} - filter value: {FilerValue}", ToSend * 2, "Alabama");


        await Task.Delay(1000).ConfigureAwait(false);
        await producer.Close().ConfigureAwait(false);
        await system.Close().ConfigureAwait(false);
    }
}
