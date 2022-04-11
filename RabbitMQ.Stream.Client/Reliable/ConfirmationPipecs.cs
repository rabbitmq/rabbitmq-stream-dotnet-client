// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Timers;
using Timer = System.Timers.Timer;

namespace RabbitMQ.Stream.Client.Reliable;

public enum ConfirmationStatus : ushort
{
    WaitForConfirmation = 0,
    Confirmed = 1,
    TimeoutError = 2,
}

public class ConfirmationMessage
{
    public ulong PublishingId { get; internal init; }
    public Message Message { get; internal set; }
    public DateTime DateTime { get; init; }
    public ConfirmationStatus ConfirmationStatus { get; internal set; }
}

public class ConfirmationPipe
{
    private ActionBlock<Tuple<ConfirmationStatus, ConfirmationMessage>> _waitForConfirmationActionBlock;
    private readonly Dictionary<ulong, ConfirmationMessage> _waitForConfirmation = new();
    private readonly Timer _invalidateTimer = new();
    private Func<ConfirmationMessage, Task> ConfirmHandler { get; }


    public ConfirmationPipe(Func<ConfirmationMessage, Task> confirmHandler)
    {
        ConfirmHandler = confirmHandler;
    }


    public void Start()
    {
        _waitForConfirmationActionBlock = new ActionBlock<Tuple<ConfirmationStatus, ConfirmationMessage>>(
            async request =>
            {
                // if (_waitForConfirmation.Count > 5000)
                //     Console.WriteLine($"_waitForConfirmation Count: {_waitForConfirmation.Count}");
                var (confirmationStatus, confirmation) = request;
                switch (confirmationStatus)
                {
                    case ConfirmationStatus.WaitForConfirmation:
                        _waitForConfirmation.TryAdd(confirmation.PublishingId, confirmation);
                        break;
                    case ConfirmationStatus.Confirmed:
                    case ConfirmationStatus.TimeoutError:
                       _waitForConfirmation.Remove(confirmation.PublishingId, out var message);
                        if (message != null)
                        {
                            message.ConfirmationStatus = confirmationStatus;
                            ConfirmHandler?.Invoke(message);
                        }
                       break;
                }
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1
            });

        _invalidateTimer.Elapsed += OnTimedEvent;
        _invalidateTimer.Interval = 1000;
        _invalidateTimer.Enabled = false;
    }

    public void Stop()
    {
        _invalidateTimer.Enabled = false;
        _waitForConfirmationActionBlock.Complete();
    }

    private async void OnTimedEvent(object? sender, ElapsedEventArgs e)
    {
        {
            foreach (var pair in _waitForConfirmation.Where(pair => (DateTime.Now - pair.Value.DateTime).Seconds > 1))
            {
                await RemoveUnConfirmedMessage(pair.Value.PublishingId, ConfirmationStatus.TimeoutError);
            }
        }
    }

    public Task AddUnConfirmedMessage(ulong publishingId, Message message)
    {
        return _waitForConfirmationActionBlock.SendAsync(Tuple.Create(ConfirmationStatus.WaitForConfirmation,
            new ConfirmationMessage() {Message = message, PublishingId = publishingId, DateTime = DateTime.Now}));
    }

    public Task RemoveUnConfirmedMessage(ulong publishingId, ConfirmationStatus confirmationStatus)
    {
        return _waitForConfirmationActionBlock.SendAsync(
            Tuple.Create(confirmationStatus,
                new ConfirmationMessage() {PublishingId = publishingId}));
    }
}
