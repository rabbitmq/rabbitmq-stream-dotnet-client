// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class SuperStreamConsumerTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public SuperStreamConsumerTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async void NumberOfConnectionsShouldBeEqualsToThePartitions()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var connectionName = Guid.NewGuid().ToString();
        var consumer = await system.CreateSuperStreamConsumer(new SuperStreamConsumerConfig()
        {
            SuperStream = "invoices",
            ClientProvidedName = connectionName,
            OffsetSpec = await SystemUtils.OffsetsForSuperStreamConsumer(system, "invoices", new OffsetTypeFirst())
        });

        Assert.NotNull(consumer);
        SystemUtils.Wait();
        SystemUtils.WaitUntil(() => SystemUtils.ConnectionsCountByName(connectionName).Result == 3);
        Assert.Equal(ResponseCode.Ok, await consumer.Close());
        await system.Close();
    }

    [Fact]
    public async void NumberOfMessagesConsumedShouldBeEqualsToPublished()
    {
        SystemUtils.ResetSuperStreams();

        var testPassed = new TaskCompletionSource<int>();
        var listConsumed = new ConcurrentBag<string>();
        var consumedMessages = 0;
        const int NumberOfMessages = 20;
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await SystemUtils.PublishMessagesSuperStream(system, "invoices", NumberOfMessages, "", _testOutputHelper);
        var clientProvidedName = Guid.NewGuid().ToString();

        var consumer = await system.CreateSuperStreamConsumer(new SuperStreamConsumerConfig()
        {
            SuperStream = "invoices",
            ClientProvidedName = clientProvidedName,
            OffsetSpec = await SystemUtils.OffsetsForSuperStreamConsumer(system, "invoices", new OffsetTypeFirst()),
            MessageHandler = (stream, consumer1, context, message) =>
            {
                listConsumed.Add(stream);
                Interlocked.Increment(ref consumedMessages);
                if (consumedMessages == NumberOfMessages)
                {
                    testPassed.SetResult(NumberOfMessages);
                }

                return Task.CompletedTask;
            }
        });

        Assert.NotNull(consumer);
        SystemUtils.Wait();
        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal(9, listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7, listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4, listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));
        Assert.Equal(ResponseCode.Ok, await consumer.Close());
        await system.Close();
    }

    [Fact]
    public async void RemoveOneConnectionIfaStreamIsDeleted()
    {
        SystemUtils.ResetSuperStreams();
        // When a stream is deleted, the consumer should remove the connection
        // This is to test the metadata update functionality
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientProvidedName = Guid.NewGuid().ToString();
        var consumer = await system.CreateSuperStreamConsumer(new SuperStreamConsumerConfig()
        {
            SuperStream = "invoices",
            ClientProvidedName = clientProvidedName,
        });

        Assert.NotNull(consumer);
        SystemUtils.Wait();
        SystemUtils.WaitUntil(() => SystemUtils.ConnectionsCountByName(clientProvidedName).Result == 3);
        SystemUtils.HttpDeleteQueue(SystemUtils.InvoicesStream0);
        SystemUtils.WaitUntil(() => SystemUtils.ConnectionsCountByName(clientProvidedName).Result == 2);
        SystemUtils.HttpDeleteQueue(SystemUtils.InvoicesStream1);
        SystemUtils.WaitUntil(() => SystemUtils.ConnectionsCountByName(clientProvidedName).Result == 1);
        await consumer.Close();
        // in this case we don't have any connection anymore since the super stream consumer is closed
        SystemUtils.WaitUntil(() => SystemUtils.ConnectionsCountByName(clientProvidedName).Result == 0);
        Assert.Equal(ResponseCode.Ok, await consumer.Close());
        await system.Close();
    }

    [Fact]
    public async void ValidateSuperStreamConsumer()
    {
        SystemUtils.ResetSuperStreams();

        var system = await StreamSystem.Create(new StreamSystemConfig());

        await Assert.ThrowsAsync<AggregateException>(() =>
            system.CreateSuperStreamConsumer(
                new SuperStreamConsumerConfig() { SuperStream = "invoices", IsSingleActiveConsumer = true, }));
    }

    [Serializable]
    public class SaCConsumerExpected
    {
        public bool IsSingleActiveConsumer { get; set; }
        public Dictionary<string, int> MessagesPerStream { get; set; }

        public int Consumers { get; set; }

        public int ClosedConsumers { get; set; }
    }

    private class SaCConsumerExpectedTestCases : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[]
            {
                new SaCConsumerExpected
                {
                    IsSingleActiveConsumer = true,
                    MessagesPerStream = new Dictionary<string, int>()
                    {
                        {SystemUtils.InvoicesStream0, 9},
                        {SystemUtils.InvoicesStream1, 7},
                        {SystemUtils.InvoicesStream2, 4}
                    },
                    Consumers = 3,
                    ClosedConsumers = 0,
                }
            };

            yield return new object[]
            {
                new SaCConsumerExpected
                {
                    IsSingleActiveConsumer = false,
                    MessagesPerStream = new Dictionary<string, int>()
                    {
                        {SystemUtils.InvoicesStream0, 9 * 3},
                        {SystemUtils.InvoicesStream1, 7 * 3},
                        {SystemUtils.InvoicesStream2, 4 * 3}
                    },
                    Consumers = 3,
                    ClosedConsumers = 0,
                }
            };

            yield return new object[]
            {
                new SaCConsumerExpected
                {
                    IsSingleActiveConsumer = true,
                    MessagesPerStream = new Dictionary<string, int>()
                    {
                        {SystemUtils.InvoicesStream0, 9 * 2},
                        {SystemUtils.InvoicesStream1, 7 * 2},
                        {SystemUtils.InvoicesStream2, 4 * 2}
                    },
                    Consumers = 3,
                    ClosedConsumers = 1,
                }
            };
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    [Theory]
    [ClassData(typeof(SaCConsumerExpectedTestCases))]
    public async void SaCNumberOfMessagesConsumedShouldBeEqualsToPublished(SaCConsumerExpected saCConsumerExpected)
    {
        SystemUtils.ResetSuperStreams();

        var listConsumed = new ConcurrentBag<string>();
        const int NumberOfMessages = 20;
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await SystemUtils.PublishMessagesSuperStream(system, "invoices", NumberOfMessages, "", _testOutputHelper);
        var clientProvidedName = Guid.NewGuid().ToString();
        var consumers = new Dictionary<string, IConsumer>();

        async Task<IConsumer> NewConsumer()
        {
            var iConsumer = await system.CreateSuperStreamConsumer(new SuperStreamConsumerConfig()
            {
                SuperStream = "invoices",
                ClientProvidedName = clientProvidedName,
                OffsetSpec = await SystemUtils.OffsetsForSuperStreamConsumer(system, "invoices", new OffsetTypeFirst()),
                IsSingleActiveConsumer = saCConsumerExpected.IsSingleActiveConsumer,
                Reference = "super_stream_consumer_name",
                MessageHandler = (stream, consumer1, context, message) =>
                {
                    listConsumed.Add(stream);
                    return Task.CompletedTask;
                }
            });
            return iConsumer;
        }

        for (var i = 0; i < saCConsumerExpected.Consumers; i++)
        {
            var consumer = await NewConsumer();
            consumers.Add($"consumer_{i}", consumer);
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(3));

        for (var i = 0; i < saCConsumerExpected.ClosedConsumers; i++)
        {
            await consumers[$"consumer_{i}"].Close();
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(3));
        Assert.Equal(saCConsumerExpected.MessagesPerStream[SystemUtils.InvoicesStream0],
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(saCConsumerExpected.MessagesPerStream[SystemUtils.InvoicesStream1],
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(saCConsumerExpected.MessagesPerStream[SystemUtils.InvoicesStream2],
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));

        await system.Close();
    }

    [Fact]
    public async void ReliableConsumerNumberOfMessagesConsumedShouldBeEqualsToPublished()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, 20, "", _testOutputHelper);
        var listConsumed = new ConcurrentBag<string>();
        var consumer = await ReliableConsumer.CreateReliableConsumer(new ReliableConsumerConfig()
        {
            StreamSystem = system,
            Stream = SystemUtils.InvoicesExchange,
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            MessageHandler = (stream, consumer1, context, message) =>
            {
                listConsumed.Add(stream);
                return Task.CompletedTask;
            }
        });

        SystemUtils.Wait(TimeSpan.FromSeconds(2));
        Assert.Equal(9,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));
        await consumer.Close();
        await system.Close();
    }

    [Fact]
    public async void ReliableConsumerNumberOfMessagesConsumedShouldBeEqualsToPublishedInSaC()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, 20, "", _testOutputHelper);
        var listConsumed = new ConcurrentBag<string>();
        var reference = Guid.NewGuid().ToString();
        var consumers = new List<ReliableConsumer>();

        async Task<ReliableConsumer> NewReliableConsumer(string refConsumer, string clientProvidedName,
            Func<string, string, bool, Task<IOffsetType>> consumerUpdateListener
        )
        {
            return await ReliableConsumer.CreateReliableConsumer(new ReliableConsumerConfig()
            {
                StreamSystem = system,
                Stream = SystemUtils.InvoicesExchange,
                OffsetSpec = new OffsetTypeFirst(),
                Reference = refConsumer,
                ClientProvidedName = clientProvidedName,
                IsSuperStream = true,
                IsSingleActiveConsumer = true,
                ConsumerUpdateListener = consumerUpdateListener,
                MessageHandler = async (stream, consumer1, context, message) =>
                {
                    await consumer1.StoreOffset(context.Offset);
                    listConsumed.Add(stream);
                }
            });
        }

        var clientProvidedName = $"first_{Guid.NewGuid().ToString()}";
        var consumerSingle = await NewReliableConsumer(reference, clientProvidedName, null);
        consumers.Add(consumerSingle);
        SystemUtils.Wait(TimeSpan.FromSeconds(1));
        for (var i = 0; i < 2; i++)
        {
            var consumer = await NewReliableConsumer(reference, Guid.NewGuid().ToString(),
                async (consumerRef, stream, arg3) =>
                    new OffsetTypeOffset(await system.QueryOffset(consumerRef, stream) + 1));
            consumers.Add(consumer);
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(2));
        Assert.Equal(9,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));

        SystemUtils.Wait(TimeSpan.FromSeconds(2));
        Assert.Equal(3, await SystemUtils.HttpKillConnections(clientProvidedName));
        SystemUtils.Wait(TimeSpan.FromSeconds(3));

        Assert.Equal(9,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));
        foreach (var reliableConsumer in consumers)
        {
            await reliableConsumer.Close();
        }

        await system.Close();
    }
}
