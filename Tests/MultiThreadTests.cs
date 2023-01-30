// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Threading;
using System.Threading.Tasks;
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

                if (error + confirmed == 802)
                {
                    receivedTask.SetResult(confirmed);
                }

                await Task.CompletedTask;
            }
        });

        for (var i = 0; i < 2; i++)
        {
            _ = Task.Run(async () =>
            {
                for (var j = 0; j < 401; j++)
                {
                    await producer.Send(new RabbitMQ.Stream.Client.Message(new byte[3]));
                }
            });
        }

        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(receivedTask);
        Assert.Equal(802, confirmed);
        Assert.Equal(802, receivedTask.Task.Result);
        Assert.Equal(0, error);
        await system.DeleteStream(stream);
    }
}
