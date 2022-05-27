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

/// <summary>
/// ReliableConsumer is a wrapper around the standard Consumer.
/// Main features are:
/// - Auto-reconnection if the connection is dropped
///   Automatically restart consuming from the last offset 
/// - Handle the Metadata Update. In case the stream is deleted ReliableProducer closes Producer/Connection.
///   Reconnect the Consumer if the stream still exists.
/// </summary>
public class ReliableConsumer : ReliableBase
{
    private Consumer _consumer;
    private readonly ReliableConsumerConfig _reliableConsumerConfig;
    private ulong _lastConsumerOffset = 0;
    private bool _consumedFirstTime = false;

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

    protected override async Task GetNewReliable(bool boot)
    {
        await SemaphoreSlim.WaitAsync();

        try
        {
            var offsetSpec = _reliableConsumerConfig.OffsetSpec;
            // if is not the boot time and at least one message was consumed
            // it can restart consuming from the last consumer offset + 1 (+1 since we need to consume fro the next)
            if (!boot && _consumedFirstTime)
            {
                offsetSpec = new OffsetTypeOffset(_lastConsumerOffset + 1);
            }

            _consumer = await _reliableConsumerConfig.StreamSystem.CreateConsumer(new ConsumerConfig()
            {
                Stream = _reliableConsumerConfig.Stream,
                ClientProvidedName = _reliableConsumerConfig.ClientProvidedName,
                Reference = _reliableConsumerConfig.Reference,
                OffsetSpec = offsetSpec,
                ConnectionClosedHandler = async _ =>
                {
                    await TryToReconnect(_reliableConsumerConfig.ReconnectStrategy);
                },
                MetadataHandler = update =>
                {
                    HandleMetaDataMaybeReconnect(update.Stream,
                        _reliableConsumerConfig.StreamSystem).Wait();
                },
                MessageHandler = async (consumer, ctx, message) =>
                {
                    _consumedFirstTime = true;
                    _lastConsumerOffset = ctx.Offset;
                    if (_reliableConsumerConfig.MessageHandler != null)
                    {
                        await _reliableConsumerConfig.MessageHandler(consumer, ctx, message);
                    }
                }
            });
            _reliableConsumerConfig.ReconnectStrategy.WhenConnected(ToString());
        }

        catch (CreateProducerException ce)
        {
            LogEventSource.Log.LogError("ReliableConsumer closed. ", ce);
        }
        catch (Exception e)
        {
            LogEventSource.Log.LogError("Error during initialization: ", e);
            SemaphoreSlim.Release();
            await TryToReconnect(_reliableConsumerConfig.ReconnectStrategy);
        }

        SemaphoreSlim.Release();
    }

    // just close the consumer. See base/metadataupdate
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

    public override string ToString()
    {
        return $"Consumer reference: {_reliableConsumerConfig.Reference},  " +
               $"stream: {_reliableConsumerConfig.Stream} ";
    }
}
