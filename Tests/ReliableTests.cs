// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

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
    private readonly ITestOutputHelper _testOutputHelper;

    public ReliableTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public void MessageWithoutConfirmationRaiseTimeout()
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
        new Utils<List<MessagesConfirmation>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        // time out error is sent by the internal time that checks the status
        // if the message doesn't receive the confirmation within X time, the timeout error is raised.
        Assert.Equal(ConfirmationStatus.ClientTimeoutError, confirmationTask.Task.Result[0].Status);
        Assert.Equal(ConfirmationStatus.ClientTimeoutError, confirmationTask.Task.Result[1].Status);
        confirmationPipe.Stop();
    }

    [Fact]
    public void MessageConfirmationShouldHaveTheSameMessages()
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
        confirmationPipe.RemoveUnConfirmedMessage(ConfirmationStatus.Confirmed, 1, null);
        confirmationPipe.RemoveUnConfirmedMessage(ConfirmationStatus.Confirmed, 2, null);
        new Utils<List<MessagesConfirmation>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        Assert.Equal(ConfirmationStatus.Confirmed, confirmationTask.Task.Result[0].Status);
        Assert.Equal(ConfirmationStatus.Confirmed, confirmationTask.Task.Result[1].Status);
        confirmationPipe.Stop();
    }

    [Fact]
    public async void ConfirmRProducerMessages()
    {
        var testPassed = new TaskCompletionSource<bool>();
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var count = 0;
        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
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
            }
        );
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
        await producer.Close();
        await system.Close();
    }

    [Fact]
    public async void SendMessageAfterKillConnectionShouldContinueToWork()
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
        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                ClientProvidedName = clientProvidedName,
                ConfirmationHandler = _ =>
                {
                    if (Interlocked.Increment(ref count) == 10)
                    {
                        testPassed.SetResult(true);
                    }

                    return Task.CompletedTask;
                }
            }
        );
        for (var i = 0; i < 5; i++)
        {
            await producer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(6));
        Assert.Equal(1, SystemUtils.HttpKillConnections(clientProvidedName).Result);
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
    public async void ProducerHandleDeleteStreamWithMetaDataUpdate()
    {
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
        // When the stream is deleted the producer has to close the 
        // connection an become inactive.
        await system.DeleteStream(stream);

        SystemUtils.Wait(TimeSpan.FromSeconds(5));
        Assert.False(producer.IsOpen());
        await system.Close();
    }

    [Fact]
    public async void HandleChangeStreamConfigurationWithMetaDataUpdate()
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
        await producer.HandleMetaDataMaybeReconnect(stream, system);
        SystemUtils.Wait();
        Assert.True(producer.IsOpen());
        // await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async void AutoPublishIdDefaultShouldStartFromTheLast()
    {
        // RProducer automatically retrieves the last producer offset.
        // see IPublishingIdStrategy implementation
        // This tests if the the last id stored 
        // A new RProducer should restart from the last offset. 

        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var testPassed = new TaskCompletionSource<ulong>();
        var clientProviderName = Guid.NewGuid().ToString();
        var reference = Guid.NewGuid().ToString();
        var count = 0;
        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                ClientProvidedName = clientProviderName,
                Reference = reference,
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
        Assert.Equal((ulong)5, testPassed.Task.Result);

        await producer.Close();
        var testPassedSecond = new TaskCompletionSource<ulong>();
        var producerSecond = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                Reference = reference,
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
        Assert.Equal((ulong)6, testPassedSecond.Task.Result);

        await producerSecond.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async void FirstConsumeAfterKillConnectionShouldContinueToWork()
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
            }
        });
        SystemUtils.Wait(TimeSpan.FromSeconds(1));
        // in this case we kill the connection before consume consume any message
        // so it should use the selected   OffsetSpec in this case = new OffsetTypeFirst(),

        SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientProviderName).Result == 1);
        await SystemUtils.PublishMessages(system, stream, NumberOfMessages, _testOutputHelper);
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        await consumer.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async void ConsumeAfterKillConnectionShouldContinueToWork()
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
        var clientProviderName = Guid.NewGuid().ToString();
        var reference = Guid.NewGuid().ToString();
        var messagesReceived = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
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
            }
        });
        SystemUtils.Wait(TimeSpan.FromSeconds(4));
        // kill the first time 
        SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientProviderName).Result == 1);
        await SystemUtils.PublishMessages(system, stream, NumberOfMessages,
            Guid.NewGuid().ToString(),
            _testOutputHelper);
        SystemUtils.Wait(TimeSpan.FromSeconds(4));
        SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientProviderName).Result == 1);
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        // after kill the consumer must be open
        Assert.True(consumer.IsOpen());
        await consumer.Close();
        Assert.False(consumer.IsOpen());
        await system.DeleteStream(stream);
        await system.Close();
    }

    [Fact]
    public async void ConsumerHandleDeleteStreamWithMetaDataUpdate()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();
        var consumer = await Consumer.Create(
            new ConsumerConfig(system, stream) { ClientProvidedName = clientProviderName, }
        );

        Assert.True(consumer.IsOpen());
        // When the stream is deleted the consumer has to close the 
        // connection an become inactive.
        await system.DeleteStream(stream);
        SystemUtils.Wait(TimeSpan.FromSeconds(5));
        Assert.False(consumer.IsOpen());
        await system.Close();
    }

    private class MyReconnection : IReconnectStrategy
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public MyReconnection(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        ValueTask<bool> IReconnectStrategy.WhenDisconnected(string info)
        {
            _testOutputHelper.WriteLine($"MyReconnection WhenDisconnected {info}");
            return ValueTask.FromResult(false);
        }

        ValueTask IReconnectStrategy.WhenConnected(string connectionInfo)
        {
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async void OverrideDefaultRecoveryConnection()
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

        SystemUtils.WaitUntil(() => consumer.IsOpen());

        await SystemUtils.PublishMessages(system, stream, 10, _testOutputHelper);
        SystemUtils.WaitUntil(() => SystemUtils.IsConnectionOpen(clientProviderName).Result);
        Assert.True(SystemUtils.IsConnectionOpen(clientProviderName).Result);
        SystemUtils.WaitUntil(() =>
            {
                var c = SystemUtils.HttpKillConnections(clientProviderName).Result;
                return c == 1;
            }
        );

        SystemUtils.WaitUntil(() =>
            {
                var isOpen = SystemUtils.IsConnectionOpen(clientProviderName).Result;
                return !isOpen;
            }
        );

        // that's should be closed at this point 
        // since the set reconnect = false
        try
        {
            SystemUtils.WaitUntil(() => false == consumer.IsOpen());
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

        internal override Task CreateNewEntity(bool boot)
        {
            if (!_firstTime)
            {
                return Task.CompletedTask;
            }

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
    public async void RConsumerShouldStopWhenThrowUnknownException()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

        var c = new FakeThrowExceptionConsumer(new ConsumerConfig(system, stream),
            new Exception("Fake Exception"));

        await Assert.ThrowsAsync<Exception>(() => c.Init(new BackOffReconnectStrategy()));

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
    public async void RConsumerShouldBeOpenWhenThrowKnownException(Exception exception)
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var c = new FakeThrowExceptionConsumer(new ConsumerConfig(system, stream),
            exception);
        Assert.True(ReliableBase.IsAKnownException(exception));
        await c.Init(new BackOffReconnectStrategy());
        // Here the Consumer should be open
        // The exception is raised only the first time
        // so it is not propagated to the caller
        Assert.True(c.IsOpen());
        await c.Close();
        Assert.False(c.IsOpen());
        await system.DeleteStream(stream);
        await system.Close();
    }
}

internal class ReliableExceptionTestCases : IEnumerable<object[]>
{
    public IEnumerator<object[]> GetEnumerator()
    {
        yield return new object[] { new LeaderNotFoundException(" Leader Not Found") };
        yield return new object[] { new SocketException(3) };
        yield return new object[] { new TimeoutException(" TimeoutException") };
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
