﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class FlowControlTests(ITestOutputHelper testOutputHelper)
{
    [Theory]
    [InlineData(ConsumerFlowStrategy.CreditsAfterParseChunk)]
    [InlineData(ConsumerFlowStrategy.CreditsBeforeParseChunk)]
    [InlineData(ConsumerFlowStrategy.ConsumerCredits)]
    public async Task ConsumerShouldConsumeMessagesWithAllFlowStrategy(ConsumerFlowStrategy strategy)
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        await SystemUtils.PublishMessages(system, stream, 130, "1", testOutputHelper);
        await SystemUtils.WaitAsync(TimeSpan.FromMilliseconds(600));
        await SystemUtils.PublishMessages(system, stream, 160, "2", testOutputHelper);

        var completionSource = new TaskCompletionSource<int>();
        var consumed = 0;
        var consumerConfig = new ConsumerConfig(system, stream)
        {
            FlowControl = new FlowControl() { Strategy = strategy, },
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (_, sourceConsumer, _, _) =>
            {
                consumed++;
                if (consumed == 290)
                {
                    completionSource.TrySetResult(consumed);
                }

                switch (strategy)
                {
                    case ConsumerFlowStrategy.CreditsAfterParseChunk:
                        // No action needed, credit is requested automatically after parsing the chunk
                        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                            await sourceConsumer.Credits());
                        break;
                    case ConsumerFlowStrategy.CreditsBeforeParseChunk:
                        // No action needed, credit is requested automatically before parsing the chunk
                        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                            await sourceConsumer.Credits());
                        break;
                    case ConsumerFlowStrategy.ConsumerCredits:
                        // In manual request credit mode, we need to request credit explicitly
                        // here we simulate the finish of processing the chunk
                        if (consumed % 10 == 0)
                            await sourceConsumer.Credits().ConfigureAwait(false);

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null);
                }
            }
        };

        var consumer = await Consumer.Create(consumerConfig);
        var result = await completionSource.Task;
        Assert.Equal(290, result);
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Theory]
    [InlineData(ConsumerFlowStrategy.CreditsAfterParseChunk)]
    [InlineData(ConsumerFlowStrategy.CreditsBeforeParseChunk)]
    [InlineData(ConsumerFlowStrategy.ConsumerCredits)]
    public async Task RawConsumerShouldConsumeMessagesWithAllFlowStrategy(ConsumerFlowStrategy strategy)
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        await SystemUtils.PublishMessages(system, stream, 130, "1", testOutputHelper);
        await SystemUtils.WaitAsync(TimeSpan.FromMilliseconds(600));
        await SystemUtils.PublishMessages(system, stream, 160, "2", testOutputHelper);

        var completionSource = new TaskCompletionSource<int>();
        var consumed = 0;
        var consumerConfig = new RawConsumerConfig(stream)
        {
            FlowControl = new FlowControl() { Strategy = strategy, },
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (sourceConsumer, _, _) =>
            {
                consumed++;
                if (consumed == 290)
                    completionSource.TrySetResult(consumed);

                switch (strategy)
                {
                    case ConsumerFlowStrategy.CreditsAfterParseChunk:
                        // No action needed, credit is requested automatically after parsing the chunk
                        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                            await sourceConsumer.Credits());

                        break;
                    case ConsumerFlowStrategy.CreditsBeforeParseChunk:
                        // No action needed, credit is requested automatically before parsing the chunk
                        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                            await sourceConsumer.Credits());
                        break;
                    case ConsumerFlowStrategy.ConsumerCredits:
                        // In manual request credit mode, we need to request credit explicitly
                        // here we simulate the finish of processing the chunk
                        if (consumed % 10 == 0)
                            await sourceConsumer.Credits().ConfigureAwait(false);

                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null);
                }
            }
        };

        var consumer = await system.CreateRawConsumer(consumerConfig);
        var result = await completionSource.Task;
        Assert.Equal(290, result);
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }
}
