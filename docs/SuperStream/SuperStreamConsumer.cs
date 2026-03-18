// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
                loggerMain.LogInformation("Consumer Name {ConsumerName} " +
                                          "-Received message id: {PropertiesMessageId} body: {S}, Stream {Stream}, Offset {Offset}",
                    consumerName, message.Properties.MessageId, Encoding.UTF8.GetString(message.Data.Contents),
                    stream, context.Offset);
                //end::consumer-simple[]
                // tag::sac-manual-offset-tracking[]
                await consumerSource.StoreOffset(context.Offset).ConfigureAwait(false); // <1>
                await Task.CompletedTask.ConfigureAwait(false);
            },
            IsSingleActiveConsumer = true, // mandatory for enabling the Single Active Consumer // <2>
            ConsumerUpdateListener = async (reference, stream, isActive) => // <3>
            {
                // don't put slow code inside this callback,
                // since it runs in the socket thread, and it could impact the consumer promotion to Active.
                // In case of exception, the library will use the default behavior that is to start consuming from OffsetNext().

                loggerMain.LogInformation($"******************************************************");
                loggerMain.LogInformation("reference {Reference} stream {Stream} is active: {IsActive}", reference,
                    stream, isActive);

                if (!isActive)
                {
                    // when the consumer is not active the server is excepting a reply from the client.
                    // even the server does not use it (a.t.m.), it is mandatory to reply with an offset type.
                    return new OffsetTypeNext();
                }

                var offset = await system.TryQueryOffset(reference, stream).ConfigureAwait(false);
                if (offset == null)
                {
                    loggerMain.LogInformation("Offset not found, will use OffsetTypeNext");
                    return new OffsetTypeNext();
                }

                
                // in this example it restarts from the last offset stored + 1,
                // but it is just an example, you can decide to restart from any offset you want.
                loggerMain.LogInformation("Restart Offset {Offset}", offset.Value + 1);
                loggerMain.LogInformation($"******************************************************");

                return new OffsetTypeOffset(offset.Value + 1); // <4>
            },
            //end::sac-manual-offset-tracking[]
        }, logger).ConfigureAwait(false);
        // 
    }
}
