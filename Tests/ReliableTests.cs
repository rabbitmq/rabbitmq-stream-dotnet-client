// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Text;
using System.Threading.Tasks;
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
        var confirmationTask = new TaskCompletionSource<ConfirmationMessage>();
        var confirmationPipe = new ConfirmationPipe(confirmation =>
            {
                confirmationTask.SetResult(confirmation);
                return Task.CompletedTask;
            }
        );
        confirmationPipe.Start();
        var message = new RabbitMQ.Stream.Client.Message(Encoding.UTF8.GetBytes($"hello"));
        confirmationPipe.AddUnConfirmedMessage(1, message);
        new Utils<ConfirmationMessage>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        Assert.Equal(ConfirmationStatus.TimeoutError, confirmationTask.Task.Result.ConfirmationStatus);
        confirmationPipe.Stop();
    }
    
    
    [Fact]
    public void MessageConfirmationShouldHaveTheSameMessages()
    {
        var confirmationTask = new TaskCompletionSource<ConfirmationMessage>();
        var confirmationPipe = new ConfirmationPipe(confirmation =>
            {
                confirmationTask.SetResult(confirmation);
                return Task.CompletedTask;
            }
        );
        confirmationPipe.Start();
        var message = new RabbitMQ.Stream.Client.Message(Encoding.UTF8.GetBytes($"hello"));
        confirmationPipe.AddUnConfirmedMessage(1, message);
        confirmationPipe.RemoveUnConfirmedMessage(1, ConfirmationStatus.Confirmed);
        new Utils<ConfirmationMessage>(_testOutputHelper).WaitUntilTaskCompletes(confirmationTask);
        Assert.Equal(ConfirmationStatus.Confirmed, confirmationTask.Task.Result.ConfirmationStatus);
        confirmationPipe.Stop();
    }
    
    
}
