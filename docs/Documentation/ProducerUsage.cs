// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Documentation;

public class ProducerUsage
{
    public static async Task CreateProducer()
    {
        
        // tag::producer-creation[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);
        
        var producer = await Producer.Create( // <1>
            new ProducerConfig(
                streamSystem,
                "my-stream") // <2>
            {
                MessagesBufferSize = 
            }
        ).ConfigureAwait(false);
        
        await producer.Close().ConfigureAwait(false); // <3>
        await streamSystem.Close().ConfigureAwait(false);
        // end::producer-creation[]
    }
}
