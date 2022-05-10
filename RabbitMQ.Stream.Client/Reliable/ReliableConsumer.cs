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

public class ReliableConsumer : ReliableBase
{
    private Consumer _consumer;
    private readonly ReliableConsumerConfig _reliableConsumerConfig;
    private ulong _lastConsumerOffset = 0;
    private bool _consumed = false;

    private ReliableConsumer(ReliableConsumerConfig reliableConsumerConfig)
    {
        _reliableConsumerConfig = reliableConsumerConfig;
        ReconnectStrategy = reliableConsumerConfig.ReconnectStrategy;
    }

    public static async Task<ReliableConsumer> CreateReliableConsumer(ReliableConsumerConfig reliableConsumerConfig)
    {
        var rConsumer = new ReliableConsumer(reliableConsumerConfig);
        await rConsumer.Init();
        return rConsumer;
    }

    protected override async Task GetNewReliable(bool boot)
    {
        await SemaphoreSlim.WaitAsync();

        try
        {
            var offsetSpec = _reliableConsumerConfig.OffsetSpec;
            if (!boot)
            {
                if (_consumed)
                {
                    offsetSpec = new OffsetTypeOffset(_lastConsumerOffset + 1);
                }
            }

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
                MetadataHandler = update =>
                {
                    HandleMetaDataMaybeReconnect(update.Stream, $"Consumer {_reliableConsumerConfig.Reference}",
                        _reliableConsumerConfig.StreamSystem).Wait();
                },
                MessageHandler = async (consumer, ctx, message) =>
                {
                    _consumed = true;
                    _lastConsumerOffset = ctx.Offset;
                    await _reliableConsumerConfig.MessageHandler(consumer, ctx, message);
                }
            });
            ReconnectStrategy.WhenConnected();
        }

        catch (CreateProducerException ce)
        {
            LogEventSource.Log.LogError($"{ce}. ReliableConsumer closed");
        }
        catch (Exception e)
        {
            LogEventSource.Log.LogError($"Error during initialization: {e}.");
            SemaphoreSlim.Release();
            await TryToReconnect();
        }

        SemaphoreSlim.Release();
    }

    protected override async Task CloseReliable()
    {
        await SemaphoreSlim.WaitAsync(10);
        try
        {
            await _consumer.Close();
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public override async Task Close()
    {
        _needReconnect = false;
        await CloseReliable();
    }
}
