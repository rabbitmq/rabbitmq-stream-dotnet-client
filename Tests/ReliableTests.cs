// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

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
        var confirmationTask = new TaskCompletionSource<List<ConfirmationMessage>>();
        var l = new List<ConfirmationMessage>();
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
        confirmationPipe.AddUnConfirmedMessage(2, new List<Message>() {message});
        new Utils<List<ConfirmationMessage>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        // time out error is sent by the internal time that checks the status
        // if the message doesn't receive the confirmation within X time, the timeout error is raised.
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result[0].ConfirmationStatus);
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result[1].ConfirmationStatus);
        confirmationPipe.Stop();
    }


    [Fact]
    public void MessageConfirmationShouldHaveTheSameMessages()
    {
        var confirmationTask = new TaskCompletionSource<List<ConfirmationMessage>>();
        var l = new List<ConfirmationMessage>();
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
        confirmationPipe.AddUnConfirmedMessage(2, new List<Message>() {message});
        confirmationPipe.RemoveUnConfirmedMessage(1, ConfirmationStatus.Confirmed);
        confirmationPipe.RemoveUnConfirmedMessage(2, ConfirmationStatus.Confirmed);
        new Utils<List<ConfirmationMessage>>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        Assert.Equal(ConfirmationStatus.Confirmed, confirmationTask.Task.Result[0].ConfirmationStatus);
        Assert.Equal(ConfirmationStatus.Confirmed, confirmationTask.Task.Result[1].ConfirmationStatus);
        confirmationPipe.Stop();
    }
}
