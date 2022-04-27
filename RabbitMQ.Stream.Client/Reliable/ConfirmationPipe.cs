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
    StreamNotAvailable = 6,
    InternalError = 15,
    AccessRefused = 16,
    PreconditionFailed = 17,
    PublisherDoesNotExist = 18,
    UndefinedError = 200,
}

/// <summary>
/// MessagesConfirmation is a wrapper around the message/s
/// This class is returned to the user to understand
/// the message status. 
/// </summary>
public class MessagesConfirmation
{
    public ulong PublishingId { get; internal set; }
    public List<Message> Messages { get; internal init; }
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
    private ActionBlock<Tuple<ConfirmationStatus, ulong>> _waitForConfirmationActionBlock;
    private readonly ConcurrentDictionary<ulong, MessagesConfirmation> _waitForConfirmation = new();
    private readonly Timer _invalidateTimer = new();
    private Func<MessagesConfirmation, Task> ConfirmHandler { get; }

    public ConfirmationPipe(Func<MessagesConfirmation, Task> confirmHandler)
    {
        ConfirmHandler = confirmHandler;
    }

    public void Start()
    {
        _waitForConfirmationActionBlock = new ActionBlock<Tuple<ConfirmationStatus, ulong>>(
            request =>
            {
                var (confirmationStatus, publishingId) = request;

                _waitForConfirmation.TryRemove(publishingId, out var message);
                if (message == null)
                {
                    return;
                }

                message.Status = confirmationStatus;
                ConfirmHandler?.Invoke(message);
            }, new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                // throttling
                BoundedCapacity = 50_000
            });

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
            foreach (var pair in _waitForConfirmation.Where(pair =>
                         (DateTime.Now - pair.Value.InsertDateTime).Seconds > 2))
            {
                await RemoveUnConfirmedMessage(pair.Value.PublishingId, ConfirmationStatus.TimeoutError);
            }
        }
    }

    public void AddUnConfirmedMessage(ulong publishingId, Message message)
    {
        AddUnConfirmedMessage(publishingId, new List<Message>() { message });
    }

    public void AddUnConfirmedMessage(ulong publishingId, List<Message> messages)
    {
        _waitForConfirmation.TryAdd(publishingId,
            new MessagesConfirmation()
            {
                Messages = messages,
                PublishingId = publishingId,
                InsertDateTime = DateTime.Now
            });
    }

    public Task RemoveUnConfirmedMessage(ulong publishingId, ConfirmationStatus confirmationStatus)
    {
        return _waitForConfirmationActionBlock.SendAsync(
            Tuple.Create(confirmationStatus, publishingId));
    }
}
