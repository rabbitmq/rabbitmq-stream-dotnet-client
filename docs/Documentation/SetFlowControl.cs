// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Documentation;

public class SetFlowControl
{
    public static async Task ConsumerFlowControl()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);
        // tag::set-flow-control[]
        var consumerConfig = new ConsumerConfig(streamSystem, "MyStream")
        {
            FlowControl = new FlowControl() // <1>
            {
                Strategy = ConsumerFlowStrategy.CreditsAfterParseChunk, // <2>
            },
        };
        // end::set-flow-control[]
    }


    public static async Task ManualFlowControl()
    {
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);
        var consumed = 0;
        // tag::set-flow-control-manual[]
        var consumerConfig = new ConsumerConfig(streamSystem, "MyStream")
        {
            FlowControl = new FlowControl() // <1>
            {
                Strategy = ConsumerFlowStrategy.ConsumerCredits, // <2>
            },
            // here we simulate a manual flow control
            // when the consumer has consumed 10 messages, it will request more credits
            MessageHandler = (_, rawConsumer, _, _) => consumed++ % 10 == 0 ? rawConsumer.Credits() : // <3>
                Task.CompletedTask
        };
        // end::set-flow-control-manual[]

        var consumer = await Consumer.Create(consumerConfig).ConfigureAwait(false);
    }
}
