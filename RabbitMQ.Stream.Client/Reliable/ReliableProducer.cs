// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableProducerConfig
{
    public StreamSystem StreamSystem { get; set; }
    public string Stream { get; set; }
    public string Reference { get; set; }
    public Func<MessagesConfirmation, Task> ConfirmationHandler { get; init; }
    public string ClientProvidedName { get; set; } = "dotnet-stream-rproducer";
    public IReconnectStrategy ReconnectStrategy { get; set; } = new BackOffReconnectStrategy();
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
    private Producer _producer;
    private ulong _publishingId;
    private readonly ReliableProducerConfig _reliableProducerConfig;
    private readonly ConfirmationPipe _confirmationPipe;

    private ReliableProducer(ReliableProducerConfig reliableProducerConfig)
    {
        _reliableProducerConfig = reliableProducerConfig;
        _confirmationPipe = new ConfirmationPipe(reliableProducerConfig.ConfirmationHandler);
        _confirmationPipe.Start();
    }

    public static async Task<ReliableProducer> CreateReliableProducer(ReliableProducerConfig reliableProducerConfig)
    {
        var rProducer = new ReliableProducer(reliableProducerConfig);
        await rProducer.Init();
        return rProducer;
    }

    protected override async Task GetNewReliable(bool boot)
    {
        await SemaphoreSlim.WaitAsync();

        try
        {
            _producer = await _reliableProducerConfig.StreamSystem.CreateProducer(new ProducerConfig()
            {
                Stream = _reliableProducerConfig.Stream,
                ClientProvidedName = _reliableProducerConfig.ClientProvidedName,
                Reference = _reliableProducerConfig.Reference,
                MetadataHandler = update =>
                {
                    HandleMetaDataMaybeReconnect(update.Stream,
                        _reliableProducerConfig.StreamSystem).Wait();
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
            _reliableProducerConfig.ReconnectStrategy.WhenConnected(ToString());
            if (boot)
            {
                // Init the publishing id
                Interlocked.Exchange(ref _publishingId,
                    await _producer.GetLastPublishingId());
            }
        }

        catch (CreateProducerException ce)
        {
            LogEventSource.Log.LogError("ReliableProducer closed", ce);
        }
        catch (Exception e)
        {
            LogEventSource.Log.LogError("Error during initialization: ", e);
            SemaphoreSlim.Release();
            await TryToReconnect(_reliableProducerConfig.ReconnectStrategy);
        }

        SemaphoreSlim.Release();
    }

    protected override async Task CloseReliable()
    {
        await SemaphoreSlim.WaitAsync(10);
        try
        {
            await _producer.Close();
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
            _needReconnect = false;
            _confirmationPipe.Stop();
            await _producer.Close();
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

    public override string ToString()
    {
        return $"Producer reference: {_reliableProducerConfig.Reference}," +
               $"stream: {_reliableProducerConfig.Stream} ";
    }
}
