// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableConsumerConfig
{
    public StreamSystem StreamSystem { get; set; }
    public string Stream { get; set; }
    public string Reference { get; set; }
    public string ClientProvidedName { get; set; } = "dotnet-stream-rconusmer";
    
    public Func<Consumer, MessageContext, Message, Task> MessageHandler { get; set; }
    
    public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

    
    public IReconnectStrategy ReconnectStrategy { get; set; } = new BackOffReconnectStrategy();
}

public class ReliableConsumer
{
    private Consumer _consumer;
    private readonly ReliableConsumerConfig _reliableConsumerConfig;
    private readonly SemaphoreSlim _semConsumer = new(1);
    private bool _needReconnect = true;
    private bool _inReconnection;
    private ulong _lastConsumerOffset = 0;


    private ReliableConsumer(ReliableConsumerConfig reliableConsumerConfig)
    {
        _reliableConsumerConfig = reliableConsumerConfig;
    }

    public static async Task<ReliableConsumer> CreateReliableConsumer(ReliableConsumerConfig reliableConsumerConfig)
    {
        var rConsumer = new ReliableConsumer(reliableConsumerConfig);
        await rConsumer.Init();
        return rConsumer;
    }

    private async Task Init()
    {
        await GetNewConsumer(true);
    }


    private async Task TryToReconnect()
    {
        _inReconnection = true;
        try
        {
            _reliableConsumerConfig.ReconnectStrategy.WhenDisconnected(out var reconnect);
            if (reconnect && _needReconnect)
            {
                await GetNewConsumer(false);
            }
        }
        finally
        {
            _inReconnection = false;
        }
    }

    private async Task GetNewConsumer(bool boot)
    {
        await _semConsumer.WaitAsync();

        try
        {
            var offsetSpec = _reliableConsumerConfig.OffsetSpec;
            if (!boot)
                offsetSpec = new OffsetTypeOffset(_lastConsumerOffset);
            
            _consumer = await _reliableConsumerConfig.StreamSystem.CreateConsumer(new ConsumerConfig()
            {
                Stream = _reliableConsumerConfig.Stream,
                ClientProvidedName = _reliableConsumerConfig.ClientProvidedName,
                Reference = _reliableConsumerConfig.Reference,
                OffsetSpec = offsetSpec,
                ConnectionClosedHandler = async _ =>
                {
                    await TryToReconnect();
                },
                MetadataHandler = _ =>
                {
                    
                },
                MessageHandler  = async (consumer, ctx, message) =>
                {
                    _lastConsumerOffset = ctx.Offset;
                    await _reliableConsumerConfig.MessageHandler(consumer, ctx, message);
                }
            });
            _reliableConsumerConfig.ReconnectStrategy.WhenConnected();
        }

        catch (CreateProducerException ce)
        {
            LogEventSource.Log.LogError($"{ce}. ReliableConsumer closed");
        }
        catch (Exception e)
        {
            LogEventSource.Log.LogError($"Error during initialization: {e}.");
            _semConsumer.Release();
            await TryToReconnect();
        }

        _semConsumer.Release();
    }

    public bool IsOpen()
    {
        return _needReconnect;
    }

    public async Task Close()
    {
        await _semConsumer.WaitAsync(10);
        try
        {
            _needReconnect = false;
            await _consumer.Close();
        }
        finally
        {
            _semConsumer.Release();
        }
    }
}
