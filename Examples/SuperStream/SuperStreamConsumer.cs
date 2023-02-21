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


        loggerMain.LogInformation("Super Stream Consumer connected to RabbitMQ. ConsumerName {ConsumerName}",
            consumerName);

        var consumer = await Consumer.Create(new ConsumerConfig(system, Costants.StreamName)
        {
            IsSuperStream = true, // Mandatory for enabling the super stream
            IsSingleActiveConsumer = true, // mandatory for enabling the Single Active Consumer
            // this is mandatory for super stream single active consumer
            // must have the same ReferenceName for all the consumers
            Reference = "MyApp",
            OffsetSpec = new OffsetTypeFirst(),
            ConsumerUpdateListener = async (reference, stream, isActive) =>
            {
                loggerMain.LogInformation($"******************************************************");
                loggerMain.LogInformation("reference {Reference} stream {Stream} is active: {IsActive}", reference, stream, isActive);
                loggerMain.LogInformation($"******************************************************");
                await Task.CompletedTask.ConfigureAwait(false);
                return new OffsetTypeLast();
            },
            MessageHandler = async (stream, consumer1, context, message) =>
            {
                loggerMain.LogInformation("Consumer Name {ConsumerName} -Received message id: {PropertiesMessageId} body: {S}, Stream {Stream}", consumerName, message.Properties.MessageId, Encoding.UTF8.GetString(message.Data.Contents), stream);
                await Task.CompletedTask.ConfigureAwait(false);
            }
        }, logger).ConfigureAwait(false);
    }
}
