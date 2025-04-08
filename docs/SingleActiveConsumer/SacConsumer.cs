// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Buffers;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace SingleActiveConsumer;

public class SacConsumer
{
    public static async Task Start()
    {
        var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        var loggerConsumer = loggerFactory.CreateLogger<Consumer>();
        var loggerMain = loggerFactory.CreateLogger<StreamSystem>();


        var streamSystem = await StreamSystem.Create(new StreamSystemConfig(), loggerMain).ConfigureAwait(false);
        var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, "my-sac-stream")
        {
            Reference = "sac_consumer",
            OffsetSpec = new OffsetTypeFirst(),
            IsSingleActiveConsumer = true,
            MessageHandler = async (_, consumer, context, message) =>
            {
                var text = Encoding.UTF8.GetString(message.Data.Contents.ToArray());
                loggerConsumer.LogInformation($"The message {text} was received");

                // Store the offset of the message.
                // store offset for each message is not a good practice
                // here is only for demo purpose
                await consumer.StoreOffset(context.Offset).ConfigureAwait(false);

                await Task.CompletedTask.ConfigureAwait(false);
            },
            ConsumerUpdateListener = async (consumerRef, stream, isActive) =>
            {
                var status = isActive ? "active" : "inactive";
                loggerConsumer.LogInformation($"Consumer {consumerRef} is {status} on stream {stream}");
                if (!isActive) return new OffsetTypeNext();
                
                var offset = await streamSystem.TryQueryOffset(consumerRef, stream).ConfigureAwait(false);
                if (offset != null)
                {
                    loggerConsumer.LogInformation($"The offset for {consumerRef} on stream {stream} is {offset}");
                    return new OffsetTypeOffset(offset.Value);
                }

                loggerConsumer.LogWarning(
                    $"The offset for {consumerRef} on stream {stream} is not available, OffsetNext will be used.");
                return new OffsetTypeNext();

            }
        }, loggerConsumer).ConfigureAwait(false);
        Console.WriteLine("Consumer is running. Press [enter] to exit.");
        Console.ReadLine();
        await consumer.Close().ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
    }
}
