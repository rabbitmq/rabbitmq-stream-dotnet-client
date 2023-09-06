// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

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
using ConsumerConfig = RabbitMQ.Stream.Client.Reliable.ConsumerConfig;

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
        var consumer = await system.CreateSuperStreamConsumer(
            new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
            {
                ClientProvidedName = connectionName,
                OffsetSpec =
                    await SystemUtils.OffsetsForSuperStreamConsumer(system, "invoices", new OffsetTypeFirst())
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
        await SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, NumberOfMessages, "", _testOutputHelper);
        var clientProvidedName = Guid.NewGuid().ToString();

        var consumer = await system.CreateSuperStreamConsumer(
            new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
            {
                CheckCrcOnDelivery = true,
                ClientProvidedName = clientProvidedName,
                OffsetSpec = await SystemUtils.OffsetsForSuperStreamConsumer(system, SystemUtils.InvoicesExchange, new OffsetTypeFirst()),
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
        var consumer = await system.CreateSuperStreamConsumer(
            new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange) { ClientProvidedName = clientProvidedName, });

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
                new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange) { IsSingleActiveConsumer = true, }));
    }

    [Serializable]
    public class ConsumerExpected
    {
        public bool IsSingleActiveConsumer { get; set; }
        public Dictionary<string, int> MessagesPerStream { get; set; }

        public int Consumers { get; set; }

        public int ClosedConsumers { get; set; }
    }

    private class ConsumerExpectedTestCases : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[]
            {
                new ConsumerExpected
                {
                    IsSingleActiveConsumer = false,
                    MessagesPerStream = new Dictionary<string, int>()
                    {
                        {SystemUtils.InvoicesStream0, 9 * 2},
                        {SystemUtils.InvoicesStream1, 7 * 2},
                        {SystemUtils.InvoicesStream2, 4 * 2}
                    },
                    Consumers = 2,
                    ClosedConsumers = 0,
                }
            };

            yield return new object[]
            {
                new ConsumerExpected
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
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    /// <summary>
    /// We test the consumer functionality in different scenarios.
    /// The class ConsumerExpectedTestCases contains the different scenarios and the expected results.
    /// each consumer should have at least the same number of messages expected for stream.
    /// It could have more messages since the consumer restarts from the beginning of the stream.
    /// but it is fine.
    /// The problem is when the consumer has less messages than expected. ( check the Assert.True(..))
    /// </summary>
    /// <param name="consumerExpected"></param>
    [Theory]
    [ClassData(typeof(ConsumerExpectedTestCases))]
    public async void MoreConsumersNumberOfMessagesConsumedShouldBeEqualsToPublished(ConsumerExpected consumerExpected)
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
            var iConsumer = await system.CreateSuperStreamConsumer(
                new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
                {
                    ClientProvidedName = clientProvidedName,
                    OffsetSpec =
                        await SystemUtils.OffsetsForSuperStreamConsumer(system, "invoices", new OffsetTypeFirst()),
                    IsSingleActiveConsumer = consumerExpected.IsSingleActiveConsumer,
                    Reference = "super_stream_consumer_name",
                    MessageHandler = (stream, consumer1, context, message) =>
                    {
                        listConsumed.Add(stream);
                        return Task.CompletedTask;
                    }
                });
            return iConsumer;
        }

        for (var i = 0; i < consumerExpected.Consumers; i++)
        {
            var consumer = await NewConsumer();
            consumers.Add($"consumer_{i}", consumer);
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(3));

        for (var i = 0; i < consumerExpected.ClosedConsumers; i++)
        {
            await consumers[$"consumer_{i}"].Close();
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(3));

        Assert.True(consumerExpected.MessagesPerStream[SystemUtils.InvoicesStream0] <=
                    listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.True(consumerExpected.MessagesPerStream[SystemUtils.InvoicesStream1] <=
                    listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.True(consumerExpected.MessagesPerStream[SystemUtils.InvoicesStream2] <=
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
        var testPassed = new TaskCompletionSource<bool>();
        var consumer = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            MessageHandler = (stream, consumer1, context, message) =>
            {
                listConsumed.Add(stream);
                if (listConsumed.Count == 20)
                {
                    testPassed.SetResult(true);
                }

                return Task.CompletedTask;
            }
        });

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.True(testPassed.Task.Result);
        Assert.Equal(9,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));
        await consumer.Close();
        await system.Close();
    }

    /// <summary>
    /// The most complex test where we test the consumer with the following configuration:
    /// Single Active Consumer = true
    /// Super Stream = true
    /// There are 3 consumers and 1 is closed by killing the connection ( so it should be recreated)
    /// When the consumer is killed a second consumer becomes active and consumes the messages
    /// But given the event: consumerUpdateListener we restart from the last stored offset
    /// So the number of messages consumed should be the same as the number of messages published 
    /// </summary>
    [Fact]
    public async void ReliableConsumerNumberOfMessagesConsumedShouldBeEqualsToPublishedInSaC()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, 20, "", _testOutputHelper);
        var listConsumed = new ConcurrentBag<string>();
        var reference = Guid.NewGuid().ToString();
        var consumers = new List<Consumer>();
        var consumerMessageReceived = new TaskCompletionSource<bool>();

        async Task<Consumer> NewReliableConsumer(string refConsumer, string clientProvidedName,
            Func<string, string, bool, Task<IOffsetType>> consumerUpdateListener
        )
        {
            return await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
            {
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
                    if (listConsumed.Count == 20)
                    {
                        consumerMessageReceived.SetResult(true);
                    }
                }
            });
        }

        var clientProvidedName = $"first_{Guid.NewGuid().ToString()}";
        // this is the first consumer that is active and consumes the messages
        var consumerSingle = await NewReliableConsumer(reference, clientProvidedName, null);
        consumers.Add(consumerSingle);
        SystemUtils.Wait(TimeSpan.FromSeconds(1));
        // these are two consumers will start consume as soon as they start
        for (var i = 0; i < 2; i++)
        {
            var consumer = await NewReliableConsumer(reference, Guid.NewGuid().ToString(),
                (consumerRef, stream, arg3) => { return new Task<IOffsetType>(() => new OffsetTypeFirst()); });
            consumers.Add(consumer);
        }

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(consumerMessageReceived);
        // The sum og the messages must be 20 as the publisher published 20 messages
        Assert.Equal(9,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));

        SystemUtils.Wait(TimeSpan.FromSeconds(2));
        // we kill the connections of the first super stream consumer ( so 3 connections one per stream)
        SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientProvidedName).Result == 3);
        SystemUtils.Wait(TimeSpan.FromSeconds(3));
        //  at this point the second consumer should be active and consume the messages
        // and the consumerUpdateListener should be called and the offset should be restored
        // so the sum of the messages must be 20 as the publisher published 20 messages
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

        // just to be sure that the connections are killed
        SystemUtils.WaitUntil(() => SystemUtils.ConnectionsCountByName(clientProvidedName).Result == 0);

        await system.Close();
    }

    /// <summary>
    /// Test when a super stream consumer with the same name joins the group
    /// so we start with one consumer and then we start another consumer with the same name
    /// the second consumer should receive the messages form one of the stream partitions
    /// </summary>
    [Fact]
    public async void SaCAddNewConsumerShouldReceiveAllTheMessage()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, 20, "", _testOutputHelper);
        var listConsumed = new ConcurrentBag<string>();
        var firstConsumerMessageReceived = new TaskCompletionSource<bool>();
        const string Reference = "My-group-app";
        var firstConsumer = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            IsSingleActiveConsumer = true,
            Reference = Reference,
            MessageHandler = (stream, consumer1, context, message) =>
            {
                listConsumed.Add(stream);
                if (listConsumed.Count == 20)
                {
                    firstConsumerMessageReceived.SetResult(true);
                }

                return Task.CompletedTask;
            }
        });

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(firstConsumerMessageReceived);
        Assert.True(firstConsumerMessageReceived.Task.Result);
        // the first consumer consumes all the messages and have to be like the messages published
        Assert.Equal(20, listConsumed.Count);
        SystemUtils.Wait(TimeSpan.FromSeconds(1));
        // the second consumer joins the group and consumes the messages only from one partition
        var listSecondConsumed = new ConcurrentBag<string>();
        var secondConsumerMessageReceived = new TaskCompletionSource<bool>();

        var secondConsumer = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            IsSingleActiveConsumer = true,
            Reference = Reference,
            MessageHandler = (stream, consumer1, context, message) =>
            {
                listSecondConsumed.Add(stream);
                // When the second consumer joins the group it consumes only from one partition
                // We don't know which partition will be consumed
                // so the test is to check if there are at least 4 messages consumed
                // that is the partition with less messages
                if (listSecondConsumed.Count >= 4)
                {
                    secondConsumerMessageReceived.SetResult(true);
                }

                return Task.CompletedTask;
            }
        });

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(secondConsumerMessageReceived);
        Assert.True(secondConsumerMessageReceived.Task.Result);
        Assert.True(listSecondConsumed.Count >= 4);

        await firstConsumer.Close();
        await secondConsumer.Close();
        await system.Close();
    }
}
