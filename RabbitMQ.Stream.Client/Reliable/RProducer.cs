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
    private ulong _lastPublishingId;

    public ulong GetPublishingId()
    {
        return ++_lastPublishingId;
    }
}

internal class BackOffReconnectStrategy : IReconnectStrategy
{
    private int Tentatives { get; set; } = 1;

    public void WhenDisconnected(out bool reconnect)
    {
        Console.WriteLine($"WhenDisconnected raised - Going to reconnect, tentative: {Tentatives}");
        Tentatives <<= 1;
        Thread.Sleep(TimeSpan.FromMilliseconds(Tentatives * 100));
        reconnect = true;
    }

    public void WhenConnected()
    {
        Tentatives = 1;
    }
}

public record RProducerConfig
{
    public StreamSystem StreamSystem { get; set; }
    public string Stream { get; set; }
    public string Reference { get; set; }
    public Func<Confirmation, Task> ConfirmationHandler { get; init; }
    public string ClientProvidedName { get; set; }
    public IReconnectStrategy ReconnectStrategy { get; set; } = new BackOffReconnectStrategy();
}

public class RProducer
{
    private Producer _producer;
    private readonly AutoPublishingId _autoPublishingId;
    private readonly RProducerConfig _rProducerConfig;
    private readonly SemaphoreSlim _semProducer = new(1);
    private readonly ConfirmationPipe _confirmationPipe;
    private bool _needReconnect = true;

    private RProducer(RProducerConfig rProducerConfig)
    {
        _autoPublishingId = new AutoPublishingId();
        _rProducerConfig = rProducerConfig;
        _confirmationPipe = new ConfirmationPipe(rProducerConfig.ConfirmationHandler);
        _confirmationPipe.Start();
    }

    public static async Task<RProducer> CreateRProducer(RProducerConfig rProducerConfig)
    {
        var rProducer = new RProducer(rProducerConfig);
        await rProducer.Init();
        return rProducer;
    }

    private async Task Init()
    {
        await _semProducer.WaitAsync();
        try
        {
            _producer = await _rProducerConfig.StreamSystem.CreateProducer(new ProducerConfig()
            {
                Stream = _rProducerConfig.Stream,
                ClientProvidedName = _rProducerConfig.ClientProvidedName,
                ConnectionClosedHandler = async _ =>
                {
                    await TryToReconnect();
                },
                ConfirmHandler = confirmation =>
                {
                    _confirmationPipe.RemoveUnConfirmedMessage(confirmation.PublishingId,
                        ConfirmationStatus.Confirmed);
                }
            });
            _rProducerConfig.ReconnectStrategy.WhenConnected();
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
        _rProducerConfig.ReconnectStrategy.WhenDisconnected(out var reconnect);
        if (reconnect && _needReconnect)
        {
            await Init();
        }
    }

    public async Task Close()
    {
        await _semProducer.WaitAsync();
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
            await _producer.Send(pid, message);
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
            await _producer.Send(pid, messages, compressionType);
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
