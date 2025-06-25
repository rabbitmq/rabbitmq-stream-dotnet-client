// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
    public async Task NumberOfConnectionsShouldBeEqualsToThePartitions()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientProvidedName = Guid.NewGuid().ToString();
        var consumer = await system.CreateSuperStreamConsumer(
            new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
            {
                ClientProvidedName = clientProvidedName,
                Identifier = "super_stream_consumer_88888",
                OffsetSpec =
                    await SystemUtils.OffsetsForSuperStreamConsumer(system, SystemUtils.InvoicesExchange,
                        new OffsetTypeFirst())
            });

        Assert.NotNull(consumer);
        await SystemUtils.WaitAsync();

        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 1);
        Assert.Equal(ResponseCode.Ok, await consumer.Close());
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 0);
        Assert.Equal("super_stream_consumer_88888", consumer.Info.Identifier);

        await system.Close();
    }

    [Fact]
    public async Task NumberOfMessagesConsumedShouldBeEqualsToPublished()
    {
        await SystemUtils.ResetSuperStreams();

        var testPassed = new TaskCompletionSource<int>();
        var listConsumed = new ConcurrentBag<string>();
        var identifierReceived = "";

        var consumedMessages = 0;
        const int NumberOfMessages = 20;
        var system = await StreamSystem.Create(new StreamSystemConfig());
        // Publish to super stream hands sometimes, for unknow reason
        var publishTask = SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, NumberOfMessages,
            "", _testOutputHelper);
        if (await Task.WhenAny(publishTask, Task.Delay(10000)) != publishTask)
        {
            Assert.Fail("timed out awaiting to publish messages to super stream");
        }

        await publishTask;

        var clientProvidedName = Guid.NewGuid().ToString();

        var consumer = await system.CreateSuperStreamConsumer(
            new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
            {
                ClientProvidedName = clientProvidedName,
                Identifier = "super_stream_consumer_24680",
                OffsetSpec =
                    await SystemUtils.OffsetsForSuperStreamConsumer(system, SystemUtils.InvoicesExchange,
                        new OffsetTypeFirst()),
                MessageHandler = (stream, sourceConsumer, _, _) =>
                {
                    identifierReceived = sourceConsumer.Info.Identifier;
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
        await SystemUtils.WaitAsync();
        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal("super_stream_consumer_24680", identifierReceived);
        Assert.Equal(9, listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7, listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4, listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));
        Assert.Equal(ResponseCode.Ok, await consumer.Close());

        await system.Close();
    }

    [Fact]
    public async Task RemoveOneConnectionIfaStreamIsDeleted()
    {
        await SystemUtils.ResetSuperStreams();
        // When a stream is deleted, the consumer should remove the connection
        // This is to test the metadata update functionality
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientProvidedName = Guid.NewGuid().ToString();
        var consumer = await system.CreateSuperStreamConsumer(
            new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
            {
                ClientProvidedName = clientProvidedName,
            });

        Assert.NotNull(consumer);
        await SystemUtils.WaitAsync();
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 1);
        SystemUtils.HttpDeleteQueue(SystemUtils.InvoicesStream0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 0);
        SystemUtils.HttpDeleteQueue(SystemUtils.InvoicesStream1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 0);
        await consumer.Close();
        // in this case we don't have any connection anymore since the super stream consumer is closed
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 0);
        Assert.Equal(ResponseCode.Ok, await consumer.Close());
        await system.Close();
    }

    [Fact]
    public async Task SingleConsumerReconnectInCaseOfKillingConnection()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientProvidedName = Guid.NewGuid().ToString();

        var configuration = new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange)
        {
            ClientProvidedName = clientProvidedName,
            OffsetSpec =
                await SystemUtils.OffsetsForSuperStreamConsumer(system, "invoices", new OffsetTypeFirst())
        };

        var consumer = await system.CreateSuperStreamConsumer(configuration);
        var completed = new TaskCompletionSource<bool>();
        configuration.ConnectionClosedHandler = async (reason, stream) =>
        {
            if (reason == ConnectionClosedReason.Unexpected)
            {
                await SystemUtils.WaitAsync();
                await consumer.ReconnectPartition(
                    await system.StreamInfo(stream).ConfigureAwait(false)
                );
                await SystemUtils.WaitAsync();
                completed.SetResult(true);
            }
        };

        Assert.NotNull(consumer);
        await SystemUtils.WaitAsync();

        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 1);

        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections($"{clientProvidedName}_0").Result == 1);

        await completed.Task;

        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 1);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 1);
        Assert.Equal(ResponseCode.Ok, await consumer.Close());
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_0").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_1").Result == 0);
        await SystemUtils.WaitUntilAsync(
            () => SystemUtils.ConnectionsCountByName($"{clientProvidedName}_2").Result == 0);
        await system.Close();
    }

    [Fact]
    public async Task ValidateSuperStreamConsumer()
    {
        await SystemUtils.ResetSuperStreams();

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
                        { SystemUtils.InvoicesStream0, 9 * 2 },
                        { SystemUtils.InvoicesStream1, 7 * 2 },
                        { SystemUtils.InvoicesStream2, 4 * 2 }
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
                        { SystemUtils.InvoicesStream0, 9 * 3 },
                        { SystemUtils.InvoicesStream1, 7 * 3 },
                        { SystemUtils.InvoicesStream2, 4 * 3 }
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
    public async Task MoreConsumersNumberOfMessagesConsumedShouldBeEqualsToPublished(ConsumerExpected consumerExpected)
    {
        await SystemUtils.ResetSuperStreams();

        var listConsumed = new ConcurrentBag<string>();
        const int NumberOfMessages = 20;
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var publishToSuperStreamTask =
            SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, NumberOfMessages, "",
                _testOutputHelper);
        if (await Task.WhenAny(publishToSuperStreamTask, Task.Delay(20000)) != publishToSuperStreamTask)
        {
            Assert.Fail("timeout waiting to publish messages");
        }

        _testOutputHelper.WriteLine("awaiting publish to super stream");
        // We re-await the task so that any exceptions/cancellation is rethrown.
        await publishToSuperStreamTask;
        var clientProvidedName = Guid.NewGuid().ToString();
        var consumers = new Dictionary<string, IConsumer>();

        for (var i = 0; i < consumerExpected.Consumers; i++)
        {
            var consumer = await NewConsumer();
            consumers.Add($"consumer_{i}", consumer);
        }

        await SystemUtils.WaitAsync(TimeSpan.FromSeconds(3));

        for (var i = 0; i < consumerExpected.ClosedConsumers; i++)
        {
            await consumers[$"consumer_{i}"].Close();
            _testOutputHelper.WriteLine($"consumer_{i} closed");
        }

        await SystemUtils.WaitAsync(TimeSpan.FromSeconds(3));

        Assert.True(consumerExpected.MessagesPerStream[SystemUtils.InvoicesStream0] <=
                    listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.True(consumerExpected.MessagesPerStream[SystemUtils.InvoicesStream1] <=
                    listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.True(consumerExpected.MessagesPerStream[SystemUtils.InvoicesStream2] <=
                    listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));

        await system.Close();

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
                    MessageHandler = (stream, _, _, _) =>
                    {
                        listConsumed.Add(stream);
                        return Task.CompletedTask;
                    }
                });
            return iConsumer;
        }
    }

    [Fact]
    public async Task ReliableConsumerNumberOfMessagesConsumedShouldBeEqualsToPublished()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        _testOutputHelper.WriteLine("awaiting publish to super stream");
        var publishTask =
            SystemUtils.PublishMessagesSuperStream(system, SystemUtils.InvoicesExchange, 20, "", _testOutputHelper);
        if (await Task.WhenAny(publishTask, Task.Delay(10000)) != publishTask)
        {
            Assert.Fail("timed out awaiting to publish messages to super stream");
        }

        // re-await in case any cancellation or exception happen, it can throw
        await publishTask;

        var listConsumed = new ConcurrentBag<string>();
        var testPassed = new TaskCompletionSource<bool>();
        var config = new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            MessageHandler = (stream, _, _, _) =>
            {
                listConsumed.Add(stream);
                if (listConsumed.Count == 20)
                {
                    testPassed.SetResult(true);
                }

                return Task.CompletedTask;
            }
        };

        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += status => { statusInfoReceived.Add(status); };

        var consumer = await Consumer.Create(config);

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.True(await testPassed.Task);
        Assert.Equal(9,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream0 ? 1 : 0));
        Assert.Equal(7,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream1 ? 1 : 0));
        Assert.Equal(4,
            listConsumed.Sum(x => x == SystemUtils.InvoicesStream2 ? 1 : 0));
        await consumer.Close();
        await SystemUtils.WaitAsync();
        Assert.Equal(2, statusInfoReceived.Count);
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[1].To);

        await system.Close();
    }

    /// <summary>
    /// Test when a super stream consumer with the same name joins the group
    /// so we start with one consumer and then we start another consumer with the same name
    /// the second consumer should receive the messages form one of the stream partitions
    /// </summary>
    [Fact]
    public async Task SaCAddNewConsumerShouldReceiveAllTheMessage()
    {
        await SystemUtils.ResetSuperStreams();
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
            MessageHandler = (stream, _, _, _) =>
            {
                listConsumed.Add(stream);
                if (listConsumed.Count == 20)
                {
                    firstConsumerMessageReceived.SetResult(true);
                }

                return Task.CompletedTask;
            }
        });
        _testOutputHelper.WriteLine("awaiting first consumer to receive all messages");
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(firstConsumerMessageReceived);
        Assert.True(await firstConsumerMessageReceived.Task);
        // the first consumer consumes all the messages and have to be like the messages published
        Assert.Equal(20, listConsumed.Count);
        await SystemUtils.WaitAsync(TimeSpan.FromSeconds(1));
        // the second consumer joins the group and consumes the messages only from one partition
        var listSecondConsumed = new ConcurrentBag<string>();
        var secondConsumerMessageReceived = new TaskCompletionSource<bool>();

        var secondConsumer = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            IsSingleActiveConsumer = true,
            Reference = Reference,
            MessageHandler = (stream, _, _, _) =>
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
        Assert.True(await secondConsumerMessageReceived.Task);
        Assert.True(listSecondConsumed.Count >= 4);

        await firstConsumer.Close();
        await secondConsumer.Close();
        await system.Close();
    }

    [Fact]
    public async Task SuperConsumerShouldReceive4StatusInfo()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());

        var clientProvidedName = Guid.NewGuid().ToString();
        var config = new ConsumerConfig(system, SystemUtils.InvoicesExchange)
        {
            ClientProvidedName = clientProvidedName,
            OffsetSpec = new OffsetTypeFirst(),
            IsSuperStream = true,
            ReconnectStrategy = new TestBackOffReconnectStrategy()
        };

        var statusCompleted = new TaskCompletionSource<bool>();
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += status =>
        {
            statusInfoReceived.Add(status);
            if (statusInfoReceived.Count == 3)
            {
                statusCompleted.SetResult(true);
            }
        };

        var consumer = await Consumer.Create(config);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections($"{clientProvidedName}_0").Result == 1);
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(statusCompleted);

        Assert.Equal(3, statusInfoReceived.Count);
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[1].To);
        Assert.Equal(ChangeStatusReason.UnexpectedlyDisconnected, statusInfoReceived[1].Reason);

        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[2].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[2].To);
        Assert.Equal(ChangeStatusReason.None, statusInfoReceived[2].Reason);
        await consumer.Close();
        await SystemUtils.WaitAsync();
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[3].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[3].To);
        Assert.Equal(ChangeStatusReason.ClosedByUser, statusInfoReceived[3].Reason);
        await system.Close();
    }

    [Fact]
    public async Task SuperConsumerShouldReceiveBoolFail()
    {
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var config = new ConsumerConfig(system, "DOES_NOT_EXIST")
        {
            IsSuperStream = true,
            ReconnectStrategy = new TestBackOffReconnectStrategy()
        };

        var statusCompleted = new TaskCompletionSource<bool>();
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += status =>
        {
            statusInfoReceived.Add(status);
            if (statusInfoReceived.Count == 1)
            {
                statusCompleted.SetResult(true);
            }
        };
        await Assert.ThrowsAsync<QueryException>(async () => await Consumer.Create(config));
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(statusCompleted);
        Assert.Equal(ChangeStatusReason.BoolFailure, statusInfoReceived[0].Reason);
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[0].To);
        await system.Close();
    }

    [Fact]
    public async Task ReliableConsumerSuperStreamInfoShouldBeTheSame()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());

        var consumer =
            await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange) { IsSuperStream = true });

        Assert.Equal(SystemUtils.InvoicesExchange, consumer.Info.Stream);
        Assert.Contains(SystemUtils.InvoicesStream0, consumer.Info.Partitions);
        Assert.Contains(SystemUtils.InvoicesStream1, consumer.Info.Partitions);
        Assert.Contains(SystemUtils.InvoicesStream2, consumer.Info.Partitions);
        Assert.Equal(
            $"ConsumerInfo(Stream={SystemUtils.InvoicesExchange}, Reference=, Identifier=, Partitions={SystemUtils.InvoicesStream0},{SystemUtils.InvoicesStream1},{SystemUtils.InvoicesStream2})",
            consumer.Info.ToString());
        await consumer.Close();
    }
}
