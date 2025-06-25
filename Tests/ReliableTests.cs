// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class ReliableTests
{
    private readonly ICrc32 _crc32 = new StreamCrc32() { };
    private readonly ITestOutputHelper _testOutputHelper;

    public ReliableTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task MessageWithoutConfirmationRaiseTimeout()
    {
        var confirmationTask = new TaskCompletionSource<int>();
        var l = new List<ConfirmationStatus>();
        var confirmationPipe = new ConfirmationPipe(async confirmation =>
            {
                l.Add(confirmation.Status);
                if (l.Count == 2)
                {
                    await Task.CompletedTask;
                    confirmationTask.SetResult(2);
                }
                else
                {
                    await Task.CompletedTask;
                }
            },
            TimeSpan.FromSeconds(2), 100
        );
        confirmationPipe.Start();
        confirmationPipe.AddUnConfirmedMessage(1, new Message(Encoding.UTF8.GetBytes($"hello")));
        confirmationPipe.AddUnConfirmedMessage(2,
            new List<Message>() { new Message(Encoding.UTF8.GetBytes($"hello")) });
        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        Assert.Equal(2, await confirmationTask.Task);
        Assert.Equal(2, l.Count);
        // time out error is sent by the internal time that checks the status
        // if the message doesn't receive the confirmation within X time, the timeout error is raised.
        Assert.Equal(ConfirmationStatus.ClientTimeoutError, l[0]);
        Assert.Equal(ConfirmationStatus.ClientTimeoutError, l[1]);
        confirmationPipe.Stop();
    }

    [Fact]
    public async Task MessageConfirmationShouldHaveTheSameMessages()
    {
        var confirmationTask = new TaskCompletionSource<List<MessagesConfirmation>>();
        var l = new List<MessagesConfirmation>();
        var confirmationPipe = new ConfirmationPipe(confirmation =>
            {
                l.Add(confirmation);
                if (confirmation.PublishingId == 2)
                {
                    confirmationTask.SetResult(l);
                }

                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(2), 100
        );
        confirmationPipe.Start();
        var message = new Message(Encoding.UTF8.GetBytes($"hello"));
        confirmationPipe.AddUnConfirmedMessage(1, message);
        confirmationPipe.AddUnConfirmedMessage(2, new List<Message>() { message });
        await confirmationPipe.RemoveUnConfirmedMessage(ConfirmationStatus.Confirmed, 1, null);
        await confirmationPipe.RemoveUnConfirmedMessage(ConfirmationStatus.Confirmed, 2, null);
        new Utils<List<MessagesConfirmation>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        var result = await confirmationTask.Task;
        Assert.Equal(ConfirmationStatus.Confirmed, result[0].Status);
        Assert.Equal(ConfirmationStatus.Confirmed, result[1].Status);
        confirmationPipe.Stop();
    }

    [Fact]
    public async Task ConfirmRProducerMessages()
    {
        var testPassed = new TaskCompletionSource<bool>();
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var count = 0;
        var config = new ProducerConfig(system, stream)
        {
            MessagesBufferSize = 150,
            Identifier = "my_producer_0874",
            ConfirmationHandler = _ =>
            {
                if (Interlocked.Increment(ref count) ==
                    5 + // first five messages iteration
                    5 + // second five messages iteration with compression enabled
                    2 // batch send iteration since the messages list contains two messages
                   )
                {
                    testPassed.SetResult(true);
                }

                return Task.CompletedTask;
            }
        };
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += (status) => { statusInfoReceived.Add(status); };
        var producer = await Producer.Create(config);

        for (var i = 0; i < 5; i++)
        {
            await producer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        List<Message> messages = new()
        {
            new Message(Encoding.UTF8.GetBytes($"hello Message1")),
            new Message(Encoding.UTF8.GetBytes($"hello Message2"))
        };

        for (var i = 0; i < 5; i++)
        {
            await producer.Send(messages, CompressionType.None);
        }

        // batch send, it will produce (messages,count) confirmation
        await producer.Send(messages);

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal("my_producer_0874", producer.Info.Identifier);
        await producer.Close();
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[1].To);
        Assert.Equal(stream, statusInfoReceived[1].Stream);
        Assert.Equal(stream, statusInfoReceived[0].Stream);
        Assert.Equal("my_producer_0874", statusInfoReceived[0].Identifier);
        Assert.Equal("my_producer_0874", statusInfoReceived[1].Identifier);
        await system.Close();
    }

    [Fact]
    public async Task SendMessageAfterKillConnectionShouldContinueToWork()
    {
        // Test the auto-reconnect client
        // When the client connection is closed by the management UI
        // see HttpKillConnections/1.
        // The RProducer has to detect the disconnection and reconnect the client
        // 
        var testPassed = new TaskCompletionSource<bool>();

        var clientProvidedNameLocator = Guid.NewGuid().ToString();
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream, clientProvidedNameLocator);
        var count = 0;
        var clientProvidedName = Guid.NewGuid().ToString();
        var config =
            new ProducerConfig(system, stream)
            {
                ClientProvidedName = clientProvidedName,
                TimeoutMessageAfter = TimeSpan.FromSeconds(3),
                ConfirmationHandler = _ =>
                {
                    if (Interlocked.Increment(ref count) == 10)
                    {
                        testPassed.SetResult(true);
                    }

                    return Task.CompletedTask;
                },
                ReconnectStrategy = new TestBackOffReconnectStrategy()
            };

        var producer = await Producer.Create(config);

        for (var i = 0; i < 5; i++)
        {
            await producer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections(clientProvidedName).Result == 1);

        await SystemUtils.HttpKillConnections(clientProvidedNameLocator);

        for (var i = 0; i < 5; i++)
        {
            List<Message> messages = new() { new Message(Encoding.UTF8.GetBytes($"hello list")) };
            await producer.Send(messages, CompressionType.None);
        }

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        // here the locator connection is closed. 
        // the auto-reconnect has to connect the locator again
        await system.DeleteStream(stream);
        await producer.Close();
        await system.Close();
    }

    [Fact]
    public async Task ProducerHandleDeleteStreamWithMetaDataUpdate()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();
        var config =
            new ProducerConfig(system, stream)
            {
                ClientProvidedName = clientProviderName,
                ConfirmationHandler = _ => Task.CompletedTask
            };
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += (status) => { statusInfoReceived.Add(status); };
        var producer = await Producer.Create(config);

        Assert.True(producer.IsOpen());
        // When the stream is deleted the producer has to close the 
        // connection an become inactive.
        await system.DeleteStream(stream);

        await SystemUtils.WaitUntilAsync(() => !producer.IsOpen());

        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[1].To);
        Assert.Equal(ChangeStatusReason.MetaDataUpdate, statusInfoReceived[1].Reason);

        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[2].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[2].To);
        Assert.Equal(ChangeStatusReason.ClosedByUser, statusInfoReceived[2].Reason);

        await system.Close();
    }

    [Fact]
    public async Task HandleChangeStreamConfigurationWithMetaDataUpdate()
    {
        // When stream topology changes the MetadataUpdate is raised.
        // in this test we simulate it using await Producer:HandleMetaDataMaybeReconnect/1;
        // Producer must reconnect
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();
        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                ClientProvidedName = clientProviderName,
                ConfirmationHandler = _ => Task.CompletedTask
            }
        );

        Assert.True(producer.IsOpen());
        await producer.OnEntityClosed(system, stream,
            ChangeStatusReason.UnexpectedlyDisconnected);
        await SystemUtils.WaitAsync();
        Assert.True(producer.IsOpen());
        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async Task AutoPublishIdDefaultShouldStartFromTheLast()
    {
        // RProducer automatically retrieves the last producer offset.
        // This tests if the the last id stored 
        // A new RProducer should restart from the last offset. 
        // This test will be removed when Reference will be mandatory 
        // in the DeduplicationProducer

        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var testPassed = new TaskCompletionSource<ulong>();
        var clientProviderName = Guid.NewGuid().ToString();
        var reference = Guid.NewGuid().ToString();
        var count = 0;
        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                _reference = reference,
                ClientProvidedName = clientProviderName,
                ConfirmationHandler = confirm =>
                {
                    if (Interlocked.Increment(ref count) != 5)
                    {
                        return Task.CompletedTask;
                    }

                    Assert.Equal(ConfirmationStatus.Confirmed, confirm.Status);

                    if (confirm.Status == ConfirmationStatus.Confirmed)
                    {
                        testPassed.SetResult(confirm.PublishingId);
                    }

                    return Task.CompletedTask;
                }
            }
        );

        Assert.True(producer.IsOpen());

        for (var i = 0; i < 5; i++)
        {
            await producer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        // We check if the publishing id is actually 5
        new Utils<ulong>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal((ulong)5, await testPassed.Task);

        await producer.Close();
        var testPassedSecond = new TaskCompletionSource<ulong>();
        var producerSecond = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                _reference = reference,
                ClientProvidedName = clientProviderName,
                ConfirmationHandler = confirm =>
                {
                    testPassedSecond.SetResult(confirm.PublishingId);
                    return Task.CompletedTask;
                }
            }
        );

        // given the same reference, the publishingId should restart from the last
        // in this cas5 is 5
        await producerSecond.Send(new Message(Encoding.UTF8.GetBytes($"hello")));
        // +1 here, so 6
        new Utils<ulong>(_testOutputHelper).WaitUntilTaskCompletes(testPassedSecond);
        Assert.Equal((ulong)6, await testPassedSecond.Task);

        await producerSecond.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async Task FirstConsumeAfterKillConnectionShouldContinueToWork()
    {
        // test the consumer reconnection 
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var testPassed = new TaskCompletionSource<bool>();
        const int NumberOfMessages = 20;
        var clientProviderName = Guid.NewGuid().ToString();
        var reference = Guid.NewGuid().ToString();
        var messagesReceived = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            Crc32 = _crc32,
            Reference = reference,
            ClientProvidedName = clientProviderName,
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (streamC, _, _, _) =>
            {
                if (Interlocked.Increment(ref messagesReceived) >= NumberOfMessages)
                {
                    testPassed.SetResult(true);
                }

                Assert.Equal(stream, streamC);
                await Task.CompletedTask;
            },
            ReconnectStrategy = new TestBackOffReconnectStrategy()
        });
        // in this case we kill the connection before consume consume any message
        // so it should use the selected   OffsetSpec in this case = new OffsetTypeFirst(),

        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections(clientProviderName).Result == 1);
        await SystemUtils.PublishMessages(system, stream, NumberOfMessages, _testOutputHelper);
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        await consumer.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async Task ConsumeAfterKillConnectionShouldContinueToWork()
    {
        // test the consumer reconnection 
        // in this test we kill the connection two times 
        // and publish two times 
        // the consumer should receive the NumberOfMessages * 2

        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        const int NumberOfMessages = 10;
        await SystemUtils.PublishMessages(system, stream, NumberOfMessages,
            Guid.NewGuid().ToString(),
            _testOutputHelper);
        var testPassed = new TaskCompletionSource<bool>();
        var statusCompleted = new TaskCompletionSource<bool>();
        var clientProviderName = Guid.NewGuid().ToString();
        var reference = Guid.NewGuid().ToString();
        var messagesReceived = 0;
        var config = new ConsumerConfig(system, stream)
        {
            Crc32 = _crc32,
            Reference = reference,
            ClientProvidedName = clientProviderName,
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (streamC, _, ctx, _) =>
            {
                // ctx.Offset starts from zero
                // here we check if the offset is NumberOfMessages *2 ( we publish two times)
                if (Interlocked.Increment(ref messagesReceived) == (NumberOfMessages * 2))
                {
                    testPassed.SetResult(true);
                }

                Assert.Equal(stream, streamC);
                await Task.CompletedTask;
            },
            ReconnectStrategy = new TestBackOffReconnectStrategy()
        };

        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += (status) =>
        {
            statusInfoReceived.Add(status);
            if (statusInfoReceived.Count == 5)
                statusCompleted.SetResult(true);
        };

        var consumer = await Consumer.Create(config);
        // kill the first time 
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections(clientProviderName).Result == 1);
        await SystemUtils.PublishMessages(system, stream, NumberOfMessages,
            Guid.NewGuid().ToString(),
            _testOutputHelper);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections(clientProviderName).Result == 1);
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        // after kill the consumer must be open
        Assert.True(consumer.IsOpen());
        // there is check to have the statusInfoReceived consistent and to 
        // to have the test passed.
        // In a real situation the test isOpen is always correct but the internal status
        // is not updated yet. Since the status Reconnection is considered as a valid Open() status  
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(statusCompleted);

        await consumer.Close();
        Assert.False(consumer.IsOpen());
        // We must have 6 status here 
        Assert.Equal(6, statusInfoReceived.Count);
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[1].To);
        Assert.Equal(ChangeStatusReason.UnexpectedlyDisconnected, statusInfoReceived[1].Reason);

        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[2].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[2].To);
        Assert.Equal(ChangeStatusReason.None, statusInfoReceived[2].Reason);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[3].From);
        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[3].To);
        Assert.Equal(ChangeStatusReason.UnexpectedlyDisconnected, statusInfoReceived[3].Reason);

        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[4].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[4].To);
        Assert.Equal(ChangeStatusReason.None, statusInfoReceived[4].Reason);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[5].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[5].To);
        Assert.Equal(ChangeStatusReason.ClosedByUser, statusInfoReceived[5].Reason);

        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async Task ConsumerHandleDeleteStreamWithMetaDataUpdate()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();
        var config =
            new ConsumerConfig(system, stream)
            {
                ClientProvidedName = clientProviderName,
                ReconnectStrategy = new TestBackOffReconnectStrategy()
            };
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += (status) => { statusInfoReceived.Add(status); };
        var consumer = await Consumer.Create(config);

        Assert.True(consumer.IsOpen());
        // When the stream is deleted the consumer has to close the 
        // connection an become inactive.
        await system.DeleteStream(stream);
        await SystemUtils.WaitUntilAsync(() => !consumer.IsOpen());
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);
        Assert.Equal(ChangeStatusReason.None, statusInfoReceived[0].Reason);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[1].To);
        Assert.Equal(ChangeStatusReason.MetaDataUpdate, statusInfoReceived[1].Reason);

        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[2].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[2].To);
        Assert.Equal(ChangeStatusReason.ClosedByUser, statusInfoReceived[2].Reason);
        await system.Close();
    }

    [Fact]
    public async Task ConsumerShouldReceiveBoolFail()
    {
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var config = new ConsumerConfig(system, "DOES_NOT_EXIST")
        {
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
        await Assert.ThrowsAsync<CreateConsumerException>(async () => await Consumer.Create(config));
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(statusCompleted);
        Assert.Equal(ChangeStatusReason.BoolFailure, statusInfoReceived[0].Reason);
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[0].To);
        await system.Close();
    }

    [Fact]
    public async Task ProducerShouldReceiveBoolFail()
    {
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var config = new ProducerConfig(system, "DOES_NOT_EXIST")
        {
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
        await Assert.ThrowsAsync<CreateProducerException>(async () => await Producer.Create(config));
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(statusCompleted);
        Assert.Equal(ChangeStatusReason.BoolFailure, statusInfoReceived[0].Reason);
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[0].To);
        await system.Close();
    }

    private class MyReconnection : IReconnectStrategy
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public MyReconnection(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        ValueTask<bool> IReconnectStrategy.WhenDisconnected(string resourceIdentifier)
        {
            _testOutputHelper.WriteLine($"MyReconnection WhenDisconnected {resourceIdentifier}");
            return ValueTask.FromResult(false);
        }

        ValueTask IReconnectStrategy.WhenConnected(string connectionInfo)
        {
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task OverrideDefaultRecoveryConnection()
    {
        // testing the ReconnectStrategy override with a new 
        // class MyReconnection. In this case we don't want the reconnection
        // the first time the client is disconnected we set reconnect = false;
        // so the Consumer just close the connection

        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();

        var consumer = await Consumer.Create(
            new ConsumerConfig(system, stream)
            {
                ClientProvidedName = clientProviderName,
                ReconnectStrategy = new MyReconnection(_testOutputHelper),
                Reference = Guid.NewGuid().ToString()
            }
        );

        await SystemUtils.WaitUntilAsync(() => consumer.IsOpen());

        await SystemUtils.PublishMessages(system, stream, 10, _testOutputHelper);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.IsConnectionOpen(clientProviderName).Result);
        Assert.True(await SystemUtils.IsConnectionOpen(clientProviderName));
        await SystemUtils.WaitUntilAsync(() =>
            {
                var c = SystemUtils.HttpKillConnections(clientProviderName).Result;
                return c == 1;
            }
        );

        await SystemUtils.WaitUntilAsync(() =>
            {
                var isOpen = SystemUtils.IsConnectionOpen(clientProviderName).Result;
                return !isOpen;
            }
        );

        // that's should be closed at this point 
        // since the set reconnect = false
        try
        {
            await SystemUtils.WaitUntilAsync(() => false == consumer.IsOpen());
        }
        finally
        {
            await system.DeleteStream(stream);
            await system.Close();
        }
    }

    private class FakeThrowExceptionConsumer : Consumer
    {
        private readonly Exception _exceptionType;
        private bool _firstTime = true;

        internal FakeThrowExceptionConsumer(ConsumerConfig consumerConfig, Exception exceptionType)
            : base(
                consumerConfig)
        {
            _exceptionType = exceptionType;
        }

        protected override Task<Info> CreateNewEntity(bool boot)
        {
            if (!_firstTime)
            {
                return null;
            }

            UpdateStatus(ReliableEntityStatus.Open, ChangeStatusReason.None);
            // raise the exception only one time
            // to avoid loops
            _firstTime = false;
            throw _exceptionType;
        }
    }

    [Fact]
    //<summary>
    // This test is to check the behavior of the Consumer when the
    // CreateNewEntity throws an exception.
    // An example could be a GrantException when the consumer is not authorized
    // The Consumer should be closed and the exception should be
    // propagated to the caller.
    //</summary>
    public async Task RConsumerShouldStopWhenThrowUnknownException()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

        var c = new FakeThrowExceptionConsumer(new ConsumerConfig(system, stream),
            new Exception("Fake Exception"));

        await Assert.ThrowsAsync<Exception>(() =>
            c.Init(new ConsumerConfig(system, stream)));

        Assert.False(c.IsOpen());

        await system.DeleteStream(stream);
        await system.Close();
    }

    [Theory]
    [ClassData(typeof(ReliableExceptionTestCases))]
    //<summary>
    // This test is to check the behavior of the Consumer when the
    // CreateNewEntity throws an known exception.
    // In this case the Consumer should reconnect due of known exception.
    // For example SocketException means that the endpoint is not available
    // it could be a temporary problem so the Consumer should try to
    // reconnect.
    //</summary>
    public async Task ConsumerShouldFailFast(Exception exception)
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var config = new ConsumerConfig(system, stream) { ReconnectStrategy = new TestBackOffReconnectStrategy() };

        var c = new FakeThrowExceptionConsumer(config,
            exception);
        Assert.True(ClientExceptions.IsAKnownException(exception));

        try
        {
            await c.Init(new ConsumerConfig(system, stream));
        }
        catch (Exception e)
        {
            Assert.True(ClientExceptions.IsAKnownException(e));
        }

        Assert.False(c.IsOpen());
        await system.DeleteStream(stream);
        await system.Close();
    }
}

internal class ReliableExceptionTestCases : IEnumerable<object[]>
{
    public IEnumerator<object[]> GetEnumerator()
    {
        yield return [new LeaderNotFoundException(" Leader Not Found")];
        yield return [new SocketException(3)];
        yield return [new TimeoutException(" TimeoutException")];
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
