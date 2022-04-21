// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
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
            }
        );
        confirmationPipe.Start();
        var message = new Message(Encoding.UTF8.GetBytes($"hello"));
        confirmationPipe.AddUnConfirmedMessage(1, message);
        confirmationPipe.AddUnConfirmedMessage(2, new List<Message>() { message });
        new Utils<List<MessagesConfirmation>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        // time out error is sent by the internal time that checks the status
        // if the message doesn't receive the confirmation within X time, the timeout error is raised.
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result[0].Status);
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result[1].Status);
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
            }
        );
        confirmationPipe.Start();
        var message = new Message(Encoding.UTF8.GetBytes($"hello"));
        confirmationPipe.AddUnConfirmedMessage(1, message);
        confirmationPipe.AddUnConfirmedMessage(2, new List<Message>() { message });
        confirmationPipe.RemoveUnConfirmedMessage(1, ConfirmationStatus.Confirmed);
        confirmationPipe.RemoveUnConfirmedMessage(2, ConfirmationStatus.Confirmed);
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
        var rProducer = await ReliableProducer.CreateReliableProducer(
            new ReliableProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
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
            await rProducer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        List<Message> messages = new() { new Message(Encoding.UTF8.GetBytes($"hello list")) };

        for (var i = 0; i < 5; i++)
        {
            await rProducer.Send(messages, CompressionType.None);
        }

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        await rProducer.Close();
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
        var rProducer = await ReliableProducer.CreateReliableProducer(
            new ReliableProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
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
            await rProducer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        SystemUtils.Wait(TimeSpan.FromSeconds(6));
        Assert.Equal(1, SystemUtils.HttpKillConnections(clientProvidedName).Result);
        await SystemUtils.HttpKillConnections(clientProvidedNameLocator);

        for (var i = 0; i < 5; i++)
        {
            List<Message> messages = new() { new Message(Encoding.UTF8.GetBytes($"hello list")) };
            await rProducer.Send(messages, CompressionType.None);
        }

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        // here the locator connection is closed. 
        // the auto-reconnect has to connect the locator again
        await system.DeleteStream(stream);
        await rProducer.Close();
        await system.Close();
    }

    [Fact]
    public async void HandleDeleteStreamWithMetaDataUpdate()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();
        var rProducer = await ReliableProducer.CreateReliableProducer(
            new ReliableProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
                ClientProvidedName = clientProviderName,
                ConfirmationHandler = _ => Task.CompletedTask
            }
        );

        Assert.True(rProducer.IsOpen());
        // When the stream is deleted the producer has to close the 
        // connection an become inactive.
        await system.DeleteStream(stream);

        SystemUtils.Wait(TimeSpan.FromSeconds(5));
        Assert.False(rProducer.IsOpen());
        await system.Close();
    }

    [Fact]
    public async void HandleChangeStreamConfigurationWithMetaDataUpdate()
    {
        // When stream topology changes the MetadataUpdate is raised.
        // in this test we simulate it using await ReliableProducer:HandleMetaDataMaybeReconnect/1;
        // Producer must reconnect
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var clientProviderName = Guid.NewGuid().ToString();
        var rProducer = await ReliableProducer.CreateReliableProducer(
            new ReliableProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
                ClientProvidedName = clientProviderName,
                ConfirmationHandler = _ => Task.CompletedTask
            }
        );

        Assert.True(rProducer.IsOpen());
        await rProducer.HandleMetaDataMaybeReconnect(stream);
        SystemUtils.Wait();
        Assert.True(rProducer.IsOpen());
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
        var rProducer = await ReliableProducer.CreateReliableProducer(
            new ReliableProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
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

        Assert.True(rProducer.IsOpen());

        for (var i = 0; i < 5; i++)
        {
            await rProducer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        // We check if the publishing id is actually 5
        new Utils<ulong>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal((ulong)5, testPassed.Task.Result);

        await rProducer.Close();
        var testPassedSecond = new TaskCompletionSource<ulong>();
        var rProducerSecond = await ReliableProducer.CreateReliableProducer(
            new ReliableProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
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
        await rProducerSecond.Send(new Message(Encoding.UTF8.GetBytes($"hello")));
        // +1 here, so 6
        new Utils<ulong>(_testOutputHelper).WaitUntilTaskCompletes(testPassedSecond);
        Assert.Equal((ulong)6, testPassedSecond.Task.Result);

        await rProducerSecond.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }
}
