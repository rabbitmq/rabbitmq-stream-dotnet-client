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
                   
                // OffsetSpec = new 
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
    
    
}
