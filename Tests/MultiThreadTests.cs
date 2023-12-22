// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

// Test some multi-threading scenarios
public class MultiThreadTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public MultiThreadTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    /// <summary>
    /// Producer send messages in multi threads
    /// Messages internally should be send one at a time
    /// The best way to use the producer is to send messages in one thread
    /// but we have to guarantee the multi-threading scenario
    /// In this PR https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/220
    /// we improved the producer to send messages in multi threads avoiding the
    /// client timeout error
    /// </summary>
    [Fact]
    public async Task PublishMessagesInMultiThreads()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        const int TotalMessages = 1000;
        const int ThreadNumber = 3;
        var receivedTask = new TaskCompletionSource<int>();
        var confirmed = 0;
        var error = 0;
        var producer = await Producer.Create(new ProducerConfig(system, stream)
        {
            ConfirmationHandler = async confirmation =>
            {
                switch (confirmation.Status)
                {
                    case ConfirmationStatus.Confirmed:
                        Interlocked.Increment(ref confirmed);
                        break;

                    default:
                        Interlocked.Increment(ref error);
                        break;
                }

                if (confirmed == TotalMessages * ThreadNumber)
                {
                    receivedTask.SetResult(confirmed);
                }

                await Task.CompletedTask;
            }
        });

        for (var i = 0; i < ThreadNumber; i++)
        {
            _ = Task.Run(async () =>
            {
                for (var j = 0; j < TotalMessages; j++)
                {
                    await producer.Send(new Message(new byte[3]));
                }
            });
        }

        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(receivedTask);
        Assert.Equal(TotalMessages * ThreadNumber, confirmed);
        Assert.Equal(TotalMessages * ThreadNumber, receivedTask.Task.Result);
        Assert.Equal(0, error);
        await producer.Close().ConfigureAwait(false);
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task CloseProducersConsumersInMultiThreads()
    {
        // This test is to verify that the producer and consumer can be closed in multi threads
        // without any error.
        // See https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/230 for more details
        // The producers/consumers should not go in hang state
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var producers = new List<Producer>();
        for (var i = 0; i < 10; i++)
        {
            var producer = await Producer.Create(new ProducerConfig(system, stream));
            producers.Add(producer);

            _ = Task.Run(async () =>
            {
                await Task.Delay(200);
                await producer.Close();
            });
            for (var j = 0; j < 10000; j++)
            {
                try
                {
                    await producer.Send(new Message(new byte[3]));
                }
                catch (Exception e)
                {
                    Assert.True(e is AlreadyClosedException);
                }
            }

            SystemUtils.WaitUntil(() => producers.TrueForAll(c => !c.IsOpen()));
            Assert.All(producers, p => Assert.False(p.IsOpen()));
        }

        var consumers = new List<Consumer>();
        for (var i = 0; i < 10; i++)
        {
            var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
            {
                OffsetSpec = new OffsetTypeFirst(),
                MessageHandler = async (stream1, consumer1, context, message) => { await Task.CompletedTask; }
            });
            consumers.Add(consumer);
            _ = Task.Run(async () =>
            {
                await Task.Delay(200);
                await consumer.Close();
            });
        }

        SystemUtils.WaitUntil(() => consumers.TrueForAll(c => !c.IsOpen()));
        Assert.All(consumers, c => Assert.False(c.IsOpen()));
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }
}
