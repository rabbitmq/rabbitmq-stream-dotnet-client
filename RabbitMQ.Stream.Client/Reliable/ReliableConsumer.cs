// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record ReliableConsumerConfig : ReliableConfig
{
    public string Reference { get; set; }
    public string ClientProvidedName { get; set; } = "dotnet-stream-rconusmer";

    public Func<string, Consumer, MessageContext, Message, Task> MessageHandler { get; set; }

    public bool IsSuperStream { get; set; }
    public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

    public bool IsSingleActiveConsumer { get; set; } = false;
    public Func<string, string, bool, Task<IOffsetType>> ConsumerUpdateListener { get; set; } = null;
}

/// <summary>
/// ReliableConsumer is a wrapper around the standard Consumer.
/// Main features are:
/// - Auto-reconnection if the connection is dropped
///   Automatically restart consuming from the last offset 
/// - Handle the Metadata Update. In case the stream is deleted ReliableProducer closes Producer/Connection.
///   Reconnect the Consumer if the stream still exists.
/// </summary>
public class ReliableConsumer : ConsumerFactory
{
    private IConsumer _consumer;

    internal ReliableConsumer(ReliableConsumerConfig reliableConsumerConfig)
    {
        _reliableConsumerConfig = reliableConsumerConfig;
    }

    public static async Task<ReliableConsumer> CreateReliableConsumer(ReliableConsumerConfig reliableConsumerConfig)
    {
        var rConsumer = new ReliableConsumer(reliableConsumerConfig);
        await rConsumer.Init(reliableConsumerConfig.ReconnectStrategy);
        return rConsumer;
    }

    internal override async Task CreateNewEntity(bool boot)
    {
        _consumer = await CreateConsumer(boot);
        await _reliableConsumerConfig.ReconnectStrategy.WhenConnected(ToString());
    }

    // just close the consumer. See base/metadataupdate
    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync(10);
        try
        {
            if (_consumer != null)
            {
                await _consumer.Close();
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public override async Task Close()
    {
        _isOpen = false;
        await CloseEntity();
    }

    public override string ToString()
    {
        return $"Consumer reference: {_reliableConsumerConfig.Reference},  " +
               $"stream: {_reliableConsumerConfig.Stream} ";
    }
}
