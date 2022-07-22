// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class SacTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public SacTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async void SaCOnlyOneActiveDefault()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        const int TotalMessages = 100;
        await SystemUtils.PublishMessages(system, stream, TotalMessages, _testOutputHelper);
        var testPassedConsumer1 = new TaskCompletionSource<int>();
        var consumer1 = await system.CreateConsumer(new ConsumerConfig()
        {
            Stream = stream,
            Reference = "TestApp1",
            OffsetSpec = new OffsetTypeFirst(),
            IsSingleActiveConsumer = true,
            MessageHandler = (_, context, _) =>
            {
                if (context.Offset == TotalMessages - 1)
                {
                    testPassedConsumer1.SetResult(TotalMessages);
                }

                return Task.CompletedTask;
            },
        });

        // var testPassedConsumer2 = new TaskCompletionSource<int>();
        // var consumer2 = await system.CreateConsumer(new ConsumerConfig()
        // {
        //     Stream = stream,
        //     Reference = "TestApp1",
        //     OffsetSpec = new OffsetTypeFirst(),
        //     IsSingleActiveConsumer = true,
        //     MessageHandler = (_, context, _) =>
        //     {
        //         if (context.Offset == TotalMessages - 1)
        //         {
        //             testPassedConsumer2.SetResult(TotalMessages);
        //         }
        //
        //         return Task.CompletedTask;
        //     },
        // });

        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(testPassedConsumer1);
        Assert.Equal(testPassedConsumer1.Task.Result, TotalMessages);
        // Assert.Equal(testPassedConsumer2.Task.Result, (int)0);
        //   await consumer1.Close();

        await system.DeleteStream(stream);
        await system.Close();
    }
}
