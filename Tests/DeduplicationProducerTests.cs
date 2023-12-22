// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class DeduplicationProducerTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public DeduplicationProducerTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task ValidateDeduplicationProducer()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        Assert.Throws<ArgumentException>(() => new DeduplicatingProducerConfig(system, stream, null));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            // reference is white space, not valid
            await DeduplicatingProducer.Create(new DeduplicatingProducerConfig(system, stream, " ")));
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task GetLastIdShouldBeEqualtoTheMessagesSent()
    {
        // here we create a producer with a reference
        // the reference is used to  enable the deduplication
        // then we query the sequence externally form the producer to be sure that
        // the values are the same
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var testPassed = new TaskCompletionSource<ulong>();
        const ulong TotalMessages = 1000UL;
        var p = await DeduplicatingProducer.Create(
            new DeduplicatingProducerConfig(system, stream, "my_producer_reference")
            {
                ConfirmationHandler = async confirmation =>
                {
                    if (confirmation.PublishingId == TotalMessages)
                        testPassed.SetResult(TotalMessages);
                    await Task.CompletedTask;
                },
            });
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            await p.Send(i, new Message(new byte[10]));
        }

        new Utils<ulong>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        SystemUtils.Wait();
        Assert.Equal(TotalMessages, await p.GetLastPublishedId());
        await p.Close();
        Assert.False(p.IsOpen());

        // here we query the sequence externally form the producer to be sure that
        // the values are the same
        Assert.Equal(TotalMessages, await system.QuerySequence("my_producer_reference", stream));
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task DeduplicationInActionSendingTheSameIdMessagesWontStore()
    {
        // in this test we send the same messages again with the same publishing id
        // to see the deduplication in action

        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var testPassed = new TaskCompletionSource<ulong>();
        const ulong TotalMessages = 1000UL;
        var p = await DeduplicatingProducer.Create(
            new DeduplicatingProducerConfig(system, stream, "my_producer_reference")
            {
                ConfirmationHandler = async confirmation =>
                {
                    if (confirmation.PublishingId == TotalMessages)
                        testPassed.SetResult(TotalMessages);
                    await Task.CompletedTask;
                },
            });
        // first send and the messages are stored
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            await p.Send(i, new Message(new byte[10]));
        }

        new Utils<ulong>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        SystemUtils.Wait();
        Assert.Equal(TotalMessages, await p.GetLastPublishedId());

        // we send the same messages again with the same publishing id
        // so the messages won't be stored due of the deduplication
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            await p.Send(i, new Message(new byte[10]));
        }

        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(stream) == (int)TotalMessages);

        // we are out of the deduplication window so the messages will be stored
        // we start from the last published id + 1
        await p.Send(await p.GetLastPublishedId() + 1, new Message(new byte[10]));
        await p.Send(await p.GetLastPublishedId() + 2, new Message(new byte[10]));

        // the total messages should be the TotalMessages + 2 new messages
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(stream) == (int)TotalMessages + 2);
        await p.Close();
        Assert.False(p.IsOpen());

        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task DeduplicationInActionSuperStream()
    {
        // Test that deduplication works with super streams
        // We send the same messages again with the same publishing id
        // to see the deduplication in action
        // the second loop will be skipped due to the deduplication
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var testPassed = new TaskCompletionSource<int>();
        const int TotalMessages = 20;
        var confirmed = 0;
        var deduplicatingProducer = await DeduplicatingProducer.Create(
            new DeduplicatingProducerConfig(system, SystemUtils.InvoicesExchange, "my_producer_reference")
            {
                SuperStreamConfig = new SuperStreamConfig()
                {
                    Routing = message1 => message1.Properties.MessageId.ToString()
                },
                ConfirmationHandler = async confirmation =>
                {
                    if (Interlocked.Increment(ref confirmed) == TotalMessages * 2)
                        testPassed.SetResult(TotalMessages);
                    await Task.CompletedTask;
                },
            });

        // send two time the same messages with the same publishing id
        for (var z = 0; z < 2; z++)
        {
            // first time the messages are stored correctly
            // second time the messages are skipped due to the deduplication
            for (ulong i = 0; i < TotalMessages; i++)
            {
                var message = new Message(Encoding.Default.GetBytes("hello"))
                {
                    Properties = new Properties() { MessageId = $"hello{i}" }
                };
                await deduplicatingProducer.Send(i, message);
            }

            // so even if there are two sends the messages count won't change
            SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
            SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
            SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        }

        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal(TotalMessages, testPassed.Task.Result);
        await deduplicatingProducer.Close();
    }
}
