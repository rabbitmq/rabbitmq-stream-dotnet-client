// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class SacTests
{
    private readonly ITestOutputHelper _testOutputHelper;
    private const int TotalMessages = 100;
    private const string AppName = "TestApp";

    public SacTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async void ValidateSaCConsumer()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            system.CreateRawConsumer(
                new RawConsumerConfig(stream) { IsSingleActiveConsumer = true, }));

        await Assert.ThrowsAsync<ArgumentException>(() => Consumer.Create(
            new ConsumerConfig(system, stream) { IsSingleActiveConsumer = true, }));

        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async void SecondConsumerActiveWhenTheFirstIsClosed()
    {
        // Tests the standard behavior of a single active consumer.
        // The first consumer consumes the entire stream. (TotalMessages)
        // since OffsetSpec = new OffsetTypeFirst(),
        // The second consumer is blocked until the first consumer is closed.
        // In this case the second consumer should consume the same number of messages as the first consumer.
        // Since the offsetSpecSecondConsumer is set to OffsetTypeFirst().
        await BaseSacTest(new OffsetTypeFirst(),
            new OffsetTypeFirst(),
            TotalMessages, Guid.NewGuid().ToString());
    }

    [Fact]
    public async void SecondConsumerActiveWhenTheFirstIsClosedAndConsume10Messages()
    {
        // Tests the standard behavior of a single active consumer.
        // The first consumer consumes the entire stream. (TotalMessages)
        // since OffsetSpec = new OffsetTypeFirst(),
        // The second consumer is blocked until the first consumer is closed.
        // In this case the second consumer should consume only 10 messages.
        // Since the offsetSpecSecondConsumer is set to OffsetTypeOffset(90).
        await BaseSacTest(new OffsetTypeFirst(),
            new OffsetTypeOffset(90),
            10, Guid.NewGuid().ToString());
    }

    [Fact]
    public async void SecondConsumerActiveWhenTheFirstIsClosedAndConsumeFromFunctionFirst()
    {
        // Tests the standard behavior of a single active consumer.
        // The first consumer consumes the entire stream. (TotalMessages)
        // since OffsetSpec = new OffsetTypeFirst(),
        // The second consumer is blocked until the first consumer is closed.
        // In this case the second consumer should read all the stream.
        // since we override the standard behavior with the ConsumerUpdateListener
        // with OffsetTypeFirst().
        await BaseSacTest(
            new OffsetTypeFirst(),
            new OffsetTypeFirst(),
            TotalMessages,
            Guid.NewGuid().ToString(),
            (s, s1, arg3) => Task.FromResult<IOffsetType>(new OffsetTypeFirst()));
    }

    [Fact]
    public async void SecondConsumerShouldConsumeFromStoredOffset()
    {
        // tests the override of the standard behavior for the second.
        // consumer with a function for ConsumerUpdateListener
        // in this case we query the `QueryOffset` 
        // and the second consumer should consume only one message.

        var streamName = Guid.NewGuid().ToString();
        var config = new StreamSystemConfig { };
        var system = await StreamSystem.Create(config);
        await BaseSacTest(
            new OffsetTypeFirst(),
            new OffsetTypeFirst(),
            1,
            streamName, async (reference, stream, isActive) =>
            {
                Assert.Equal(AppName, reference);
                Assert.Equal(streamName, stream);
                if (isActive)
                {
                    return await Task.FromResult<IOffsetType>(
                        new OffsetTypeOffset(await system.QueryOffset(reference, stream)));
                }

                return null;
            });
    }

    private async Task BaseSacTest(IOffsetType offsetSpecFirstConsumer,
        IOffsetType offsetSpecSecondConsumer,
        int messagesSecondConsumer,
        string stream, Func<string, string, bool, Task<IOffsetType>> offsetTypeFunc = null)
    {
        var system = await StreamSystem.Create(new StreamSystemConfig { });
        await system.CreateStream(new StreamSpec(stream));

        await SystemUtils.PublishMessages(system, stream, TotalMessages, _testOutputHelper);
        var testPassedConsumer1 = new TaskCompletionSource<int>();
        var consumer1 = await system.CreateRawConsumer(new RawConsumerConfig(stream)
        {
            Reference = AppName,
            OffsetSpec = offsetSpecFirstConsumer,
            IsSingleActiveConsumer = true,
            MessageHandler = async (consumer, context, _) =>
            {
                // we store the offset because we need to use 
                // in some test when we want to restart from the 
                // last offset stored
                await consumer.StoreOffset(context.Offset);
                if (context.Offset == TotalMessages - 1)
                {
                    testPassedConsumer1.SetResult(TotalMessages);
                }
            },
        });

        var testPassedConsumer2 = new TaskCompletionSource<int>();
        var messagesConsumed = 0;
        var consumer2 = await system.CreateRawConsumer(new RawConsumerConfig(stream)
        {
            Reference = AppName,
            OffsetSpec = offsetSpecSecondConsumer,
            ConsumerUpdateListener = offsetTypeFunc,
            IsSingleActiveConsumer = true,
            MessageHandler = (_, context, _) =>
            {
                Interlocked.Increment(ref messagesConsumed);
                // here when the first consumer is closed,
                // we must be sure that we are in the end of the stream
                // before checking if the second consumer consumed the 
                // messages we expected. (messagesConsumed)
                if (context.Offset != TotalMessages - 1)
                {
                    return Task.CompletedTask;
                }

                if (messagesConsumed == messagesSecondConsumer)
                {
                    testPassedConsumer2.SetResult(messagesConsumed);
                }

                return Task.CompletedTask;
            },
        });

        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(testPassedConsumer1);
        // Here the consumer1 is still active, so the consumers should be blocked.
        Assert.Equal(testPassedConsumer1.Task.Result, TotalMessages);
        // Just to be sure that testPassedConsumer2 is not called.
        SystemUtils.Wait();
        // so the testPassedConsumer2 should stay in WaitingForActivation state.
        // since the second consumer is blocked.
        Assert.Equal(TaskStatus.WaitingForActivation, testPassedConsumer2.Task.Status);
        await consumer1.Close();
        // at this point the consumer2 should be activated since the consumer1 is closed.
        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(testPassedConsumer2);
        // messagesSecondConsumer is the number of messages that the second consumer should consume.
        // based on offsetSpecSecondConsumer
        Assert.Equal(testPassedConsumer2.Task.Result, messagesSecondConsumer);
        await consumer2.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }
}
