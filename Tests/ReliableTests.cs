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
using Confirmation = RabbitMQ.Stream.Client.Reliable.Confirmation;

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
        var confirmationTask = new TaskCompletionSource<List<Confirmation>>();
        var l = new List<Confirmation>();
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
        new Utils<List<Confirmation>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        // time out error is sent by the internal time that checks the status
        // if the message doesn't receive the confirmation within X time, the timeout error is raised.
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result[0].Status);
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result[1].Status);
        confirmationPipe.Stop();
    }

    [Fact]
    public void MessageConfirmationShouldHaveTheSameMessages()
    {
        var confirmationTask = new TaskCompletionSource<List<Confirmation>>();
        var l = new List<Confirmation>();
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
        new Utils<List<Confirmation>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
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
        var rProducer = await RProducer.CreateRProducer(
            new RProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
                ConfirmationHandler = confirmation =>
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
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var count = 0;
        var rProducer = await RProducer.CreateRProducer(
            new RProducerConfig()
            {
                Stream = stream,
                StreamSystem = system,
                ClientProvidedName = "producer_to_kill",
                ConfirmationHandler = confirmation =>
                {
                    if (Interlocked.Increment(ref count) == 5)
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
        Assert.Equal(1, SystemUtils.HttpKillConnections("producer_to_kill").Result);
        Assert.Equal(1, SystemUtils.HttpKillConnections("dotnet-stream-locator").Result);

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
}
