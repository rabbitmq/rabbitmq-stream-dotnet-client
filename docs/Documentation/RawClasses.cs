// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using RabbitMQ.Stream.Client;

namespace Documentation;

public class RawClasses
{
    public static async Task CreateRawProducer()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);
        // tag::raw-producer-creation[]
        var rawProducer = await streamSystem.CreateRawProducer( // <1>
            new RawProducerConfig("my-stream")
            {
                ConnectionClosedHandler = async reason => // <2>
                {
                    Console.WriteLine($"Connection closed with reason: {reason}");
                    await Task.CompletedTask.ConfigureAwait(false);
                },
                MetadataHandler = update => // <3>
                {
                    Console.WriteLine($"Metadata Stream updated: {update.Stream}");
                },
                ConfirmHandler = confirmation => // <4>
                {
                    Console.WriteLine(confirmation.Code == ResponseCode.Ok
                        ? $"Message confirmed: {confirmation.PublishingId}"
                        : $"Message: {confirmation.PublishingId} not confirmed with error: {confirmation.Code}");
                }
            }
            // end::raw-producer-creation[]
        ).ConfigureAwait(false); // <1> 
    }
}
