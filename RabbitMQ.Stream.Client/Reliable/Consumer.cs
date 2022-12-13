// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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

    // <summary>
    // Function where the consumer receives the messages.
    // Parameters:
    // - string: the stream name where the message was published
    // - MessageContext: Context of the message, see <see cref="RawConsumer.MessageContext"/>
    // - Message: the decode message received
    // </summary>
    public Func<string, RawConsumer, MessageContext, Message, Task> MessageHandler { get; set; }

    // <summary>
    // Enable the SuperStream stream feature.
    // See also: https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams
    // Parameters:
    public bool IsSuperStream { get; set; }

    // <summary>
    // The offset is the place in the stream where the consumer starts consuming from. The possible values for the offset parameter are the following:
    // - OffsetTypeFirst: starting from the first available offset. If the stream has not been truncated, this means the beginning of the stream (offset 0).
    // - OffsetTypeLast: starting from the end of the stream and returning the last chunk of messages immediately (if the stream is not empty).
    // - OffsetTypeNext: starting from the next offset to be written. Contrary to OffsetTypeLast, consuming with OffsetTypeNext will not return anything if no-one is publishing to the stream. The broker will start sending messages to the consumer when messages are published to the stream.
    // - OffsetTypeOffset(offset): starting from the specified offset. 0 means consuming from the beginning of the stream (first messages). The client can also specify any number, for example the offset where it left off in a previous incarnation of the application.
    // - OffsetTypeTimeStamp(timestamp): starting from the messages stored after the specified timestamp.
    // </summary>
    public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

    // <summary>
    // When the single active consumer feature is enabled for several consumer instances sharing the same stream and name, only one of these instances will be active at a time and so will receive messages.
    // The other instances will be idle.
    // </summary>
    public bool IsSingleActiveConsumer { get; set; } = false;

    // <summary>
    // The broker notifies a consumer that becomes active before dispatching messages to it. 
    // With ConsumerUpdateListener the consumer can decide where to start consuming from.
    // The event is raised only in case of single active consumer.
    // </summary>
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
    private readonly ILogger<Consumer> _logger;

    protected override ILogger BaseLogger => _logger;

    internal Consumer(ConsumerConfig consumerConfig, ILogger<Consumer> logger = null)
    {
        _logger = logger ?? NullLogger<Consumer>.Instance;
        _consumerConfig = consumerConfig;
    }

    public static async Task<Consumer> Create(ConsumerConfig consumerConfig, ILogger<Consumer> logger = null)
    {
        consumerConfig.ReconnectStrategy ??= new BackOffReconnectStrategy(logger);
        var rConsumer = new Consumer(consumerConfig, logger);
        await rConsumer.Init(consumerConfig.ReconnectStrategy);
        logger?.LogDebug("Consumer: {Reference} created for Stream: {Stream}",
            consumerConfig.Reference, consumerConfig.Stream);

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
        _logger?.LogDebug("Consumer closed for stream {Stream}", _consumerConfig.Stream);
    }

    public override string ToString()
    {
        return $"Consumer reference: {_consumerConfig.Reference}, stream: {_consumerConfig.Stream} ";
    }
}
