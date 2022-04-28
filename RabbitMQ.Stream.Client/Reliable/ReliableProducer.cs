// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

internal class AutoPublishingId : IPublishingIdStrategy
{
    private ulong _lastPublishingId = 0;
    private readonly ReliableProducerConfig _reliableProducerConfig;

    public ulong GetPublishingId()
    {
        return ++_lastPublishingId;
    }

    public AutoPublishingId(ReliableProducerConfig reliableProducerConfig)
    {
        _reliableProducerConfig = reliableProducerConfig;
    }

    public async Task InitPublishingId()
    {
        try
        {
            _lastPublishingId =
                await _reliableProducerConfig.StreamSystem.QuerySequence(_reliableProducerConfig.Reference,
                    _reliableProducerConfig.Stream);
        }
        catch (Exception)
        {
            _lastPublishingId = 0;
        }
    }
}

internal class BackOffReconnectStrategy : IReconnectStrategy
{
    private int Tentatives { get; set; } = 1;

    public void WhenDisconnected(out bool reconnect)
    {
        Tentatives <<= 1;
        LogEventSource.Log.LogInformation(
            $"Producer disconnected, check if reconnection needed in {Tentatives * 100} ms.");
        Thread.Sleep(TimeSpan.FromMilliseconds(Tentatives * 100));
        reconnect = true;
    }

    public void WhenConnected()
    {
        Tentatives = 1;
    }
}

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
public class ReliableProducer
{
    private Producer _producer;
    private readonly AutoPublishingId _autoPublishingId;
    private readonly ReliableProducerConfig _reliableProducerConfig;
    private readonly SemaphoreSlim _semProducer = new(1);
    private readonly ConfirmationPipe _confirmationPipe;
    private bool _needReconnect = true;
    private bool _inReconnection;

    private ReliableProducer(ReliableProducerConfig reliableProducerConfig)
    {
        _reliableProducerConfig = reliableProducerConfig;
        _autoPublishingId = new AutoPublishingId(_reliableProducerConfig);
        _confirmationPipe = new ConfirmationPipe(reliableProducerConfig.ConfirmationHandler);
        _confirmationPipe.Start();
    }

    public static async Task<ReliableProducer> CreateReliableProducer(ReliableProducerConfig reliableProducerConfig)
    {
        var rProducer = new ReliableProducer(reliableProducerConfig);
        await rProducer.Init();
        return rProducer;
    }

    private async Task Init()
    {
        await _autoPublishingId.InitPublishingId();
        await GetNewProducer();
    }

    private async Task GetNewProducer()
    {
        await _semProducer.WaitAsync();

        try
        {
            _producer = await _reliableProducerConfig.StreamSystem.CreateProducer(new ProducerConfig()
            {
                Stream = _reliableProducerConfig.Stream,
                ClientProvidedName = _reliableProducerConfig.ClientProvidedName,
                Reference = _reliableProducerConfig.Reference,
                MetadataHandler = update =>
                {
                    HandleMetaDataMaybeReconnect(update.Stream).Wait();
                },
                ConnectionClosedHandler = async _ =>
                {
                    await TryToReconnect();
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
            _reliableProducerConfig.ReconnectStrategy.WhenConnected();
        }

        catch (CreateProducerException ce)
        {
            LogEventSource.Log.LogError($"{ce}. ReliableProducer closed");
        }
        catch (Exception e)
        {
            LogEventSource.Log.LogError($"Error during initialization: {e}.");
            _semProducer.Release();
            await TryToReconnect();
        }

        _semProducer.Release();
    }

    private async Task TryToReconnect()
    {
        _inReconnection = true;
        try
        {
            _reliableProducerConfig.ReconnectStrategy.WhenDisconnected(out var reconnect);
            if (reconnect && _needReconnect)
            {
                await GetNewProducer();
            }
        }
        finally
        {
            _inReconnection = false;
        }
    }

    /// <summary>
    /// When the clients receives a meta data update, it doesn't know
    /// the reason.
    /// Metadata update can be raised when:
    /// - stream is deleted
    /// - change the stream topology (ex: add a follower)
    ///
    /// HandleMetaDataMaybeReconnect checks if the stream still exists
    /// and try to reconnect.
    /// (internal because it is needed for tests)
    /// </summary>
    internal async Task HandleMetaDataMaybeReconnect(string stream)
    {
        LogEventSource.Log.LogInformation(
            $"Meta data update for the stream: {stream} " +
            $"Producer {_reliableProducerConfig.Reference} closed.");

        // This sleep is needed. When a stream is deleted it takes sometime.
        // The StreamExists/1 could return true even the stream doesn't exist anymore.
        Thread.Sleep(500);
        if (await _reliableProducerConfig.StreamSystem.StreamExists(stream))
        {
            LogEventSource.Log.LogInformation(
                $"Meta data update, the stream {stream} still exist. " +
                $"Producer {_reliableProducerConfig.Reference} will try to reconnect.");
            // Here we just close the producer connection
            // the func TryToReconnect/0 will be called. 
            await CloseProducer();
        }
        else
        {
            // In this case the stream doesn't exist anymore
            // the ReliableProducer is just closed.
            await Close();
        }
    }

    private async Task CloseProducer()
    {
        await _semProducer.WaitAsync(10);
        try
        {
            await _producer.Close();
        }
        finally
        {
            _semProducer.Release();
        }
    }

    public bool IsOpen()
    {
        return _needReconnect;
    }

    public async Task Close()
    {
        await _semProducer.WaitAsync(10);
        try
        {
            _needReconnect = false;
            _confirmationPipe.Stop();
            await _producer.Close();
        }
        finally
        {
            _semProducer.Release();
        }
    }

    public async ValueTask Send(Message message)
    {
        var pid = _autoPublishingId.GetPublishingId();
        _confirmationPipe.AddUnConfirmedMessage(pid, message);
        await _semProducer.WaitAsync();
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (!(_inReconnection))
            {
                await _producer.Send(pid, message);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError($"Error sending message: {e}.");
        }
        finally
        {
            _semProducer.Release();
        }
    }

    public async ValueTask Send(List<Message> messages, CompressionType compressionType)
    {
        var pid = _autoPublishingId.GetPublishingId();
        _confirmationPipe.AddUnConfirmedMessage(pid, messages);
        await _semProducer.WaitAsync();
        try
        {
            if (!_inReconnection)
            {
                await _producer.Send(pid, messages, compressionType);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError($"Error sending messages: {e}.");
        }
        finally
        {
            _semProducer.Release();
        }
    }
}
