// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableProducerConfig : ReliableConfig
{
    private readonly TimeSpan _timeoutMessageAfter = TimeSpan.FromSeconds(3);

    public string Reference { get; set; }
    public Func<MessagesConfirmation, Task> ConfirmationHandler { get; init; }
    public string ClientProvidedName { get; set; } = "dotnet-stream-rproducer";

    public int MaxInFlight { get; set; } = 1000;

    public TimeSpan TimeoutMessageAfter
    {
        get => _timeoutMessageAfter;
        init
        {
            if (value.TotalMilliseconds < 1000)
            {
                throw new ValidationException("TimeoutMessageAfter has to be at least 1000ms");
            }

            _timeoutMessageAfter = value;
        }
    }
}

/// <summary>
/// ReliableProducer is a wrapper around the standard Producer.
/// Main features are:
/// - Auto-reconnection if the connection is dropped
/// - Trace sent and received messages. The event ReliableProducer:ConfirmationHandler/2
///   receives back messages sent with the status.
/// - Handle the Metadata Update. In case the stream is deleted ReliableProducer closes Producer/Connection.
///   Reconnect the Producer if the stream still exists.
/// - Set automatically the next PublisherID
/// - Automatically retrieves the last sequence. By default is AutoPublishingId see IPublishingIdStrategy.
/// </summary>
public class ReliableProducer : ReliableBase
{
    private IProducer _producer;
    private ulong _publishingId;
    private readonly ReliableProducerConfig _reliableProducerConfig;
    private readonly ConfirmationPipe _confirmationPipe;

    private ReliableProducer(ReliableProducerConfig reliableProducerConfig)
    {
        _reliableProducerConfig = reliableProducerConfig;
        _confirmationPipe = new ConfirmationPipe(
            reliableProducerConfig.ConfirmationHandler,
            reliableProducerConfig.TimeoutMessageAfter,
            reliableProducerConfig.MaxInFlight);
    }

    // <summary>
    // Create a new ReliableProducer
    // </summary> 
    public static async Task<ReliableProducer> CreateReliableProducer(ReliableProducerConfig reliableProducerConfig)
    {
        var rProducer = new ReliableProducer(reliableProducerConfig);
        await rProducer.Init(reliableProducerConfig.ReconnectStrategy);
        return rProducer;
    }

    internal override async Task CreateNewEntity(bool boot)
    {
        _producer = await _reliableProducerConfig.StreamSystem.CreateProducer(new ProducerConfig()
        {
            Stream = _reliableProducerConfig.Stream,
            ClientProvidedName = _reliableProducerConfig.ClientProvidedName,
            Reference = _reliableProducerConfig.Reference,
            MaxInFlight = _reliableProducerConfig.MaxInFlight,
            MetadataHandler = update =>
            {
                // This is Async since the MetadataHandler is called from the Socket connection thread
                // HandleMetaDataMaybeReconnect/2 could go in deadlock.

                Task.Run(() =>
                {
                    // intentionally fire & forget
                    HandleMetaDataMaybeReconnect(update.Stream,
                        _reliableProducerConfig.StreamSystem).WaitAsync(CancellationToken.None);
                });
            },
            ConnectionClosedHandler = async _ =>
            {
                await TryToReconnect(_reliableProducerConfig.ReconnectStrategy);
            },
            ConfirmHandler = confirmation =>
            {
                var confirmationStatus = confirmation.Code switch
                {
                    ResponseCode.PublisherDoesNotExist => ConfirmationStatus.PublisherDoesNotExist,
                    ResponseCode.AccessRefused => ConfirmationStatus.AccessRefused,
                    ResponseCode.InternalError => ConfirmationStatus.InternalError,
                    ResponseCode.PreconditionFailed => ConfirmationStatus.PreconditionFailed,
                    ResponseCode.StreamNotAvailable => ConfirmationStatus.StreamNotAvailable,
                    ResponseCode.Ok => ConfirmationStatus.Confirmed,
                    _ => ConfirmationStatus.UndefinedError
                };

                _confirmationPipe.RemoveUnConfirmedMessage(confirmation.PublishingId,
                    confirmationStatus);
            }
        });

        await _reliableProducerConfig.ReconnectStrategy.WhenConnected(ToString());

        if (boot)
        {
            // Init the publishing id
            Interlocked.Exchange(ref _publishingId,
                await _producer.GetLastPublishingId());

            // confirmation Pipe can start only if the producer is ready
            _confirmationPipe.Start();
        }
    }

    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync(10);
        try
        {
            if (_producer != null)
            {
                await _producer.Close();
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public override async Task Close()
    {
        await SemaphoreSlim.WaitAsync(TimeSpan.FromMilliseconds(10));
        try
        {
            _isOpen = false;
            _confirmationPipe.Stop();
            if (_producer != null)
            {
                await _producer.Close();
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public async ValueTask Send(Message message)
    {
        Interlocked.Increment(ref _publishingId);
        _confirmationPipe.AddUnConfirmedMessage(_publishingId, message);
        await SemaphoreSlim.WaitAsync();
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (!(_inReconnection))
            {
                await _producer.Send(_publishingId, message);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError("Error sending message: ", e);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public async ValueTask Send(List<Message> messages, CompressionType compressionType)
    {
        Interlocked.Increment(ref _publishingId);
        _confirmationPipe.AddUnConfirmedMessage(_publishingId, messages);
        await SemaphoreSlim.WaitAsync();
        try
        {
            if (!_inReconnection)
            {
                await _producer.Send(_publishingId, messages, compressionType);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError("Error sending messages: ", e);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public async ValueTask BatchSend(List<Message> messages)
    {
        var messagesToSend = new List<(ulong, Message)>();
        foreach (var message in messages)
        {
            Interlocked.Increment(ref _publishingId);
            messagesToSend.Add((_publishingId, message));
        }

        foreach (var msg in messagesToSend)
        {
            _confirmationPipe.AddUnConfirmedMessage(msg.Item1, msg.Item2);
        }

        await SemaphoreSlim.WaitAsync();
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (!(_inReconnection))
            {
                await _producer.BatchSend(messagesToSend);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError("BatchSend error sending message: ", e);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public override string ToString()
    {
        return $"Producer reference: {_reliableProducerConfig.Reference}, stream: {_reliableProducerConfig.Stream} ";
    }
}
