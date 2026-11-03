// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;
using ConsumerConfig = RabbitMQ.Stream.Client.Reliable.ConsumerConfig;

namespace Tests;

public class SubscriptionListenerTests(ITestOutputHelper testOutputHelper)
{
    [Fact]
    public async Task StandardConsumerListenerOverridesTheOffset()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        await SystemUtils.PublishMessages(system, stream, 10, testOutputHelper);

        var contexts = new ConcurrentBag<(string, string)>();
        var consumed = 0;
        var done = new TaskCompletionSource<bool>();
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            Reference = "sub-listener",
            // default is Next: without the listener nothing would be consumed
            OffsetSpec = new OffsetTypeNext(),
            SubscriptionListener = ctx =>
            {
                contexts.Add((ctx.Stream, ctx.Reference));
                ctx.OffsetSpec = new OffsetTypeOffset(5);
                return Task.CompletedTask;
            },
            MessageHandler = (_, _, _, _) =>
            {
                if (System.Threading.Interlocked.Increment(ref consumed) == 5)
                {
                    done.SetResult(true);
                }

                return Task.CompletedTask;
            }
        });

        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal((stream, "sub-listener"), Assert.Single(contexts));
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task StandardConsumerListenerIsCalledOnRecovery()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        await SystemUtils.PublishMessages(system, stream, 10, testOutputHelper);
        var offsets = new ConcurrentQueue<IOffsetType>();
        var clientName = Guid.NewGuid().ToString();
        var consumed = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            ClientProvidedName = clientName,
            OffsetSpec = new OffsetTypeFirst(),
            SubscriptionListener = ctx =>
            {
                offsets.Enqueue(ctx.OffsetSpec);
                return Task.CompletedTask;
            },
            MessageHandler = (_, _, _, _) =>
            {
                System.Threading.Interlocked.Increment(ref consumed);
                return Task.CompletedTask;
            }
        });

        await SystemUtils.WaitUntilAsync(() => offsets.Count == 1);
        Assert.IsType<OffsetTypeFirst>(offsets.First());
        // all the messages must be dispatched before killing the connection,
        // otherwise the recovery offset is not deterministic
        await SystemUtils.WaitUntilAsync(() => System.Threading.Volatile.Read(ref consumed) == 10);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.ConnectionsCountByName(clientName).Result == 1);
        await SystemUtils.WaitUntilAsync(() => System.Threading.Volatile.Read(ref consumed) == 10);
        await SystemUtils.HttpKillConnections(clientName);
        await SystemUtils.WaitUntilAsync(() => offsets.Count == 2);
        // 10 messages consumed => recovery starts from offset 10
        Assert.Equal(10UL, Assert.IsType<OffsetTypeOffset>(offsets.Last()).OffsetValue);
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task ListenerExceptionKeepsTheConfiguredOffset()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        await SystemUtils.PublishMessages(system, stream, 3, testOutputHelper);
        var consumed = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            OffsetSpec = new OffsetTypeFirst(),
            SubscriptionListener = _ => throw new InvalidOperationException("boom"),
            MessageHandler = (_, _, _, _) =>
            {
                System.Threading.Interlocked.Increment(ref consumed);
                return Task.CompletedTask;
            }
        });
        await SystemUtils.WaitUntilAsync(() => consumed == 3);
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task SuperStreamListenerIsCalledForEachPartition()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streams = new ConcurrentBag<string>();
        var consumer = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            IsSuperStream = true,
            OffsetSpec = new OffsetTypeNext(),
            SubscriptionListener = ctx =>
            {
                streams.Add(ctx.Stream);
                ctx.OffsetSpec = new OffsetTypeFirst();
                return Task.CompletedTask;
            },
            MessageHandler = (_, _, _, _) => Task.CompletedTask
        });

        await SystemUtils.WaitUntilAsync(() => streams.Count == 3);
        Assert.Equal(3, streams.Distinct().Count());
        Assert.All(streams, s => Assert.StartsWith(SystemUtils.InvoicesExchange, s));
        await consumer.Close();
        await system.Close();
    }

    [Fact]
    public async Task StandardConsumerListenerCanQueryTheOffset()
    {
        // the listener runs while the consumer is created: querying the offset from it must not deadlock
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        ulong? queried = 42;
        var called = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            Reference = "sub-listener-query",
            SubscriptionListener = async ctx =>
            {
                Interlocked.Increment(ref called);
                queried = await system.TryQueryOffset(ctx.Reference, ctx.Stream);
            },
            MessageHandler = (_, _, _, _) => Task.CompletedTask
        }).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(1, called);
        Assert.Null(queried);
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    [Fact]
    public async Task SuperStreamListenerCanQueryTheOffset()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var called = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            Reference = "sub-listener-query",
            IsSuperStream = true,
            SubscriptionListener = async ctx =>
            {
                Interlocked.Increment(ref called);
                await system.TryQueryOffset(ctx.Reference, ctx.Stream);
            },
            MessageHandler = (_, _, _, _) => Task.CompletedTask
        }).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(3, called);
        await consumer.Close();
        await system.Close();
    }
}
