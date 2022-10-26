// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record ConsumerConfig : ReliableConfig
{
    /// <summary>
    /// Consumer reference name.
    /// Used to identify the consumer server side when store the messages offset.
    /// see also <see cref="StreamSystem.QueryOffset"/> to retrieve the last offset.
    /// </summary>
    public string Reference { get; set; }

    // <summary>
    // The client name used to identify the Consumer. 
    // You can see this value on the Management UI or in the connection detail
    // </summary>
    public string ClientProvidedName { get; set; } = "dotnet-stream-conusmer";

    public Func<string, RawConsumer, MessageContext, Message, Task> MessageHandler { get; set; }

    public bool IsSuperStream { get; set; }
    public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

    public bool IsSingleActiveConsumer { get; set; } = false;
    public Func<string, string, bool, Task<IOffsetType>> ConsumerUpdateListener { get; set; } = null;

    public ConsumerConfig(StreamSystem streamSystem, string stream) : base(streamSystem, stream)
    {
    }
}

/// <summary>
/// Consumer is a wrapper around the standard RawConsumer.
/// Main features are:
/// - Auto-reconnection if the connection is dropped
///   Automatically restart consuming from the last offset 
/// - Handle the Metadata Update. In case the stream is deleted Producer closes Producer/Connection.
///   Reconnect the Consumer if the stream still exists.
/// </summary>
public class Consumer : ConsumerFactory
{
    private IConsumer _consumer;

    internal Consumer(ConsumerConfig consumerConfig)
    {
        _consumerConfig = consumerConfig;
    }

    public static async Task<Consumer> Create(ConsumerConfig consumerConfig)
    {
        var rConsumer = new Consumer(consumerConfig);
        await rConsumer.Init(consumerConfig.ReconnectStrategy);
        return rConsumer;
    }

    internal override async Task CreateNewEntity(bool boot)
    {
        _consumer = await CreateConsumer(boot);
        await _consumerConfig.ReconnectStrategy.WhenConnected(ToString());
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
        return $"Consumer reference: {_consumerConfig.Reference},  " +
               $"stream: {_consumerConfig.Stream} ";
    }
}
