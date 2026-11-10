// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
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
        // 10 messages consumed => recovery starts from offset 9 (starting form 0)
        Assert.Equal(9UL, Assert.IsType<OffsetTypeOffset>(offsets.Last()).OffsetValue);
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
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var superStreamName = $"SuperStreamListenerIsCalledForEachPartition_super_stream_{Guid.NewGuid()}";
        await system.CreateSuperStream(new PartitionsSuperStreamSpec(superStreamName, 3));
        var streams = new ConcurrentBag<string>();
        var consumer = await Consumer.Create(new ConsumerConfig(system, superStreamName)
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
        Assert.All(streams, s => Assert.StartsWith(superStreamName, s));
        await consumer.Close();
        await system.DeleteSuperStream(superStreamName);
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
        var superStreamName = $"SuperStreamListenerCanQueryTheOffset_super_stream_{Guid.NewGuid()}";
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await system.CreateSuperStream(new PartitionsSuperStreamSpec(superStreamName, 3));
        var called = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, superStreamName)
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
        await system.DeleteSuperStream(superStreamName);
        await system.Close();
    }

    [Fact]
    public async Task SuperStreamListenerSetsADifferentOffsetForEachPartitionOnRecovery()
    {
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var mySuperStream = "SuperStreamListenerSetsADifferentOffsetForEachPartitionOnRecovery_" +
                            Guid.NewGuid().ToString("N");
        var spec = new PartitionsSuperStreamSpec(mySuperStream, 3) { };

        await system.CreateSuperStream(spec);
        var producer = await Producer.Create(new ProducerConfig(system, mySuperStream)
        {
            SuperStreamConfig = new SuperStreamConfig()
            {
                Routing = message => message.Properties.MessageId.ToString(),
                RoutingStrategyType = RoutingStrategyType.Key
            }
        });

        // routing keys are 0,1,2: 20 messages for each partition
        for (var i = 0; i < 60; i++)
        {
            await producer.Send(new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"{i % 3}" }
            });
        }

        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(mySuperStream + "-0") == 20);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(mySuperStream + "-1") == 20);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(mySuperStream + "-2") == 20);
        await producer.Close();

        // what an external offset store would return for each partition
        var storedOffsets = new Dictionary<string, ulong>
        {
            [mySuperStream + "-0"] = 17,
            [mySuperStream + "-1"] = 8,
            [mySuperStream + "-2"] = 19
        };
        var recovering = 0;
        var consumed = 0;
        var listenerCalls = new ConcurrentQueue<(string Stream, bool Recovery)>();
        var firstOffsetAfterRecovery = new ConcurrentDictionary<string, ulong>();
        var clientName = Guid.NewGuid().ToString();
        var consumer = await Consumer.Create(new ConsumerConfig(system, mySuperStream)
        {
            ClientProvidedName = clientName,
            IsSuperStream = true,
            OffsetSpec = new OffsetTypeFirst(),
            SubscriptionListener = ctx =>
            {
                var isRecovery = Volatile.Read(ref recovering) == 1;
                listenerCalls.Enqueue((ctx.Stream, isRecovery));
                if (isRecovery)
                {
                    // storedOffsets simulates an external store that returns a different offset for each partition
                    ctx.OffsetSpec = new OffsetTypeOffset(storedOffsets[ctx.Stream]);
                }

                return Task.CompletedTask;
            },
            MessageHandler = (stream, _, context, _) =>
            {
                if (Volatile.Read(ref recovering) == 1)
                {
                    firstOffsetAfterRecovery.TryAdd(stream, context.Offset);
                }
                else
                {
                    Interlocked.Increment(ref consumed);
                }

                return Task.CompletedTask;
            }
        });

        // all the messages must be consumed before killing the connections,
        // so the first messages received after the recovery come only from the new offsets
        await SystemUtils.WaitUntilAsync(() => Volatile.Read(ref consumed) == 60);
        Assert.Equal(3, listenerCalls.Count(c => !c.Recovery));
        // the partition connections are named <clientName>_<index>
        await SystemUtils.WaitUntilAsync(() => SystemUtils.IsConnectionOpen(clientName).Result);
        Volatile.Write(ref recovering, 1);
        await SystemUtils.HttpKillConnections(clientName);

        // the partitions are recovered one after the other, each one with a random delay
        await SystemUtils.WaitUntilAsync(() => firstOffsetAfterRecovery.Count == 3, 120);
        Assert.Equal(3, listenerCalls.Count(c => c.Recovery));
        foreach (var (stream, offset) in storedOffsets)
        {
            Assert.Equal(offset, firstOffsetAfterRecovery[stream]);
        }

        await system.DeleteSuperStream(mySuperStream);
        await consumer.Close();
        await system.Close();
    }
}
