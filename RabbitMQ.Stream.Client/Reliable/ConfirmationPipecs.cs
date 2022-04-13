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
/// <summary>
/// ConfirmationStatus can be:
/// </summary>
public enum ConfirmationStatus : ushort
{
    WaitForConfirmation = 0,
    Confirmed = 1,
    TimeoutError = 2,
}
/// <summary>
/// Confirmation is a wrapper around the message/s
/// This class is returned to the user to understand
/// the message status. 
/// </summary>
public class Confirmation
{
    public ulong PublishingId { get; internal init; }
    public List<Message> Messages { get; internal set; }
    public DateTime InsertDateTime { get; init; }
    public ConfirmationStatus Status { get; internal set; }
}

/// <summary>
/// ConfirmationPipe maintains the status for the sent and received messages.
/// TPL Action block sends the confirmation to the user in async way
/// So the send/1 is not blocking.
/// </summary>
public class ConfirmationPipe
{
    private ActionBlock<Tuple<ConfirmationStatus, Confirmation>> _waitForConfirmationActionBlock;
    private readonly ConcurrentDictionary<ulong, Confirmation> _waitForConfirmation = new();
    private readonly Timer _invalidateTimer = new();
    private Func<Confirmation, Task> ConfirmHandler { get; }

    public ConfirmationPipe(Func<Confirmation, Task> confirmHandler)
    {
        ConfirmHandler = confirmHandler;
    }

    public void Start()
    {
        _waitForConfirmationActionBlock = new ActionBlock<Tuple<ConfirmationStatus, Confirmation>>(
            request =>
            {
                var (confirmationStatus, confirmation) = request;
                switch (confirmationStatus)
                {
                    case ConfirmationStatus.Confirmed:
                    case ConfirmationStatus.TimeoutError:
                        _waitForConfirmation.TryRemove(confirmation.PublishingId, out var message);
                        if (message != null)
                        {
                            message.Status = confirmationStatus;
                            ConfirmHandler?.Invoke(message);
                        }
                        break;
                }
            }, new ExecutionDataflowBlockOptions { 
                MaxDegreeOfParallelism = 1,
                // throttling
                BoundedCapacity = 50_000 });

        _invalidateTimer.Elapsed += OnTimedEvent;
        _invalidateTimer.Interval = 2000;
        _invalidateTimer.Enabled = true;
    }

    public void Stop()
    {
        _invalidateTimer.Enabled = false;
        _waitForConfirmationActionBlock.Complete();
    }

    private async void OnTimedEvent(object? sender, ElapsedEventArgs e)
    {
        {
            foreach (var pair in _waitForConfirmation.Where(pair => (DateTime.Now - pair.Value.InsertDateTime).Seconds > 2))
            {
                await RemoveUnConfirmedMessage(pair.Value.PublishingId, ConfirmationStatus.TimeoutError);
            }
        }
    }

    public void AddUnConfirmedMessage(ulong publishingId, Message message)
    {
        _waitForConfirmation.TryAdd(publishingId,
            new Confirmation()
            {
                Messages = new List<Message>() { message },
                PublishingId = publishingId,
                InsertDateTime = DateTime.Now
            });
    }

    public void AddUnConfirmedMessage(ulong publishingId, List<Message> messages)
    {
        _waitForConfirmation.TryAdd(publishingId,
            new Confirmation() { Messages = messages, PublishingId = publishingId, InsertDateTime = DateTime.Now });
    }

    public Task RemoveUnConfirmedMessage(ulong publishingId, ConfirmationStatus confirmationStatus)
    {
        return _waitForConfirmationActionBlock.SendAsync(
            Tuple.Create(confirmationStatus,
                new Confirmation() { PublishingId = publishingId }));
    }
}
