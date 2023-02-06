// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStream;

public static class SuperStreamConsumer
{
    public static async Task Start(string consumerName)
    {
        Console.WriteLine("Starting SuperStream Consumer {0}", consumerName);
        var config = new StreamSystemConfig();
        var system = await StreamSystem.Create(config);

        Console.WriteLine("Super Stream Consumer connected to RabbitMQ. ConsumerName {0}", consumerName);

        var consumer = await Consumer.Create(new ConsumerConfig(system, Costants.StreamName)
        {
            IsSuperStream = true, // Mandatory for enabling the super stream
            IsSingleActiveConsumer = true, // mandatory for enabling the Single Active Consumer
            // this is mandatory for super stream single active consumer
            // must have the same ReferenceName for all the consumers
            Reference = "MyApp",
            OffsetSpec = new OffsetTypeFirst(),
            
            MessageHandler = async (stream, consumer1, context, message) =>
            {
                Console.WriteLine(
                    $"Consumer Name {consumerName} -Received message id: {message.Properties.MessageId} body: {Encoding.UTF8.GetString(message.Data.Contents)}, Stream {stream}");
                await Task.CompletedTask.ConfigureAwait(false);
            }
        }).ConfigureAwait(false);
    }
}
