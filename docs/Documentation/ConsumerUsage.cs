// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Documentation;

public class ConsumerUsage
{
    public static async Task CreateConsumer()
    {
        // tag::consumer-creation[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var consumer = await Consumer.Create( // <1>
            new ConsumerConfig( // <2>
                streamSystem,
                "my-stream")
            {
                // Reference = 
                OffsetSpec = new OffsetTypeTimestamp(), // <3>
                MessageHandler = async (stream, consumer, context, message) => // <4>
                {
                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Data.Contents)}");
                    await Task.CompletedTask.ConfigureAwait(false);
                }
            }
        ).ConfigureAwait(false);

        await consumer.Close().ConfigureAwait(false); // <5>
        await streamSystem.Close().ConfigureAwait(false);
        // end::consumer-creation[]
    }


    public static async Task CreateConsumerManualOffsetTrack()
    {
        // tag::manual-tracking-defaults[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var consumed = 0;
        var consumer = await Consumer.Create(
            new ConsumerConfig(
                streamSystem,
                "my-stream")
            {
                Reference = "my-reference", // <1>
                MessageHandler = async (stream, consumer, context, message) =>
                {
                    if (consumed++ % 10000 == 0)
                    {
                        await consumer.StoreOffset(context.Offset).ConfigureAwait(false); // <2>
                    }

                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Data.Contents)}");
                    await Task.CompletedTask.ConfigureAwait(false);
                }
            }
        ).ConfigureAwait(false);

        await consumer.Close().ConfigureAwait(false); // <5>
        await streamSystem.Close().ConfigureAwait(false);
        // end::manual-tracking-defaults[]
    }


    public static async Task CreateConsumerSingle()
    {
        // tag::enabling-single-active-consumer[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var consumer = await Consumer.Create(
            new ConsumerConfig(
                streamSystem,
                "my-stream")
            {
                Reference = "my-reference", // <1>
                IsSingleActiveConsumer = true, // <2>
                // end::enabling-single-active-consumer[]
                MessageHandler = async (stream, consumer, context, message) =>
                {
                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Data.Contents)}");
                    await Task.CompletedTask.ConfigureAwait(false);
                }
            }
        ).ConfigureAwait(false);

        await consumer.Close().ConfigureAwait(false); 
        await streamSystem.Close().ConfigureAwait(false);
        
    }
    
    
    
    public static async Task CreateConsumerSingleUpdateListener()
    {
        // tag::sac-consumer-update-listener[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var consumer = await Consumer.Create(
            new ConsumerConfig(
                streamSystem,
                "my-stream")
            {
                Reference = "my-reference", // <1>
                IsSingleActiveConsumer = true, // <2>
                ConsumerUpdateListener = async (consumerRef, stream, isActive) => // <3>
                {
                   var offset =  await streamSystem.QueryOffset(consumerRef, stream).ConfigureAwait(false);
                   return new OffsetTypeOffset(offset);
                },
                // end::sac-consumer-update-listener[]
                MessageHandler = async (stream, consumer, context, message) =>
                {
                    Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Data.Contents)}");
                    await Task.CompletedTask.ConfigureAwait(false);
                }
            }
        ).ConfigureAwait(false);

        await consumer.Close().ConfigureAwait(false); 
        await streamSystem.Close().ConfigureAwait(false);
        
    }
}
