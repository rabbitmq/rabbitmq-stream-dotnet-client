// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStream;

public class SuperStreamConsumer
{
    public static async Task Start(string consumerName)
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var logger = loggerFactory.CreateLogger<Consumer>();
        var loggerMain = loggerFactory.CreateLogger<SuperStreamConsumer>();
        loggerMain.LogInformation("Starting SuperStream Consumer {ConsumerName}", consumerName);


        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config).ConfigureAwait(false);

        Console.WriteLine("Super Stream Consumer connected to RabbitMQ. ConsumerName {0}", consumerName);
        // tag::consumer-simple[]
        var consumer = await Consumer.Create(new ConsumerConfig(system, Costants.StreamName)
        {
            IsSuperStream = true, // Mandatory for enabling the super stream // <1>
            // this is mandatory for super stream single active consumer
            // must have the same ReferenceName for all the consumers
            Reference = "MyApp",
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (stream, consumerSource, context, message) => // <2>
            {
                Console.WriteLine(
                    $"Consumer Name {consumerName} -Received message id: {message.Properties.MessageId} body: {Encoding.UTF8.GetString(message.Data.Contents)}, Stream {stream}");
                //end::consumer-simple[]
                // tag::sac-manual-offset-tracking[]
                await consumerSource.StoreOffset(context.Offset).ConfigureAwait(false); // <1>
                await Task.CompletedTask.ConfigureAwait(false);
            },
            IsSingleActiveConsumer = true, // mandatory for enabling the Single Active Consumer // <2>
            ConsumerUpdateListener = async (reference, stream, isActive) => // <3>
            {
                loggerMain.LogInformation($"******************************************************");
                loggerMain.LogInformation("reference {Reference} stream {Stream} is active: {IsActive}", reference,
                    stream, isActive);
               
                ulong offset = 0;
                try
                {
                    offset = await system.QueryOffset(reference, stream).ConfigureAwait(false); 
                }
                catch (OffsetNotFoundException e)
                {
                    loggerMain.LogInformation("OffsetNotFoundException {Message}, will use OffsetTypeNext", e.Message);
                    return new OffsetTypeNext();
                }
                loggerMain.LogInformation("Restart Offset {Offset}", offset);
                loggerMain.LogInformation($"******************************************************");
                await Task.CompletedTask.ConfigureAwait(false);
                return new OffsetTypeOffset(offset + 1);// <4>
            },
            //end::sac-manual-offset-tracking[]

        }, logger).ConfigureAwait(false);
        // 
    }
}
