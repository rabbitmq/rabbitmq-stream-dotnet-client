// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client.Reliable;

public record ConsumerConfig : ReliableConfig
{
    /// <summary>
    /// Consumer reference name.
    /// Used to identify the consumer server side when storing the messages offset.
    /// <br/>
    /// See also <see cref="StreamSystem.QueryOffset"/> to retrieve the last offset.
    /// </summary>
    public string Reference { get; set; }

    /// <summary>
    /// The client name used to identify the Consumer. 
    /// You can see this value on the Management UI or in the connection details.
    /// </summary>
    public string ClientProvidedName { get; set; } = "dotnet-stream-consumer";

    /// <summary>
    /// Callback function where the consumer receives the messages.
    /// The callback runs in a different Task respect to the socket thread.
    /// Parameters that will be received by this function:
    /// <list type="bullet">
    /// <item>
    /// string: the stream name where the message was published
    /// </item>
    /// <item>
    /// MessageContext: Context of the message, see <see cref="MessageContext"/>
    /// </item>
    /// <item>
    /// Message: the decode message received
    /// </item>
    /// </list>
    /// </summary>
    public Func<string, RawConsumer, MessageContext, Message, Task> MessageHandler { get; set; }

    /// <summary>
    /// Enable the SuperStream stream feature.
    /// <br/>
    /// <a href="https://blog.rabbitmq.com/posts/2022/07/rabbitmq-3-11-feature-preview-super-streams">
    /// Check this link for more information about it.
    /// </a>
    /// </summary>
    public bool IsSuperStream { get; set; }

    /// <summary>
    /// The offset is the place in the stream where the consumer starts consuming from.
    /// The possible values are:
    /// <list type="bullet">
    /// <item>
    /// <see cref="OffsetTypeFirst"/>: starting from the first available offset.
    /// If the stream has not been truncated, this means the beginning of the stream (offset 0).
    /// </item>
    /// <item>
    /// <see cref="OffsetTypeLast"/>: starting from the end of the stream
    /// and returning the last chunk of messages immediately (if the stream is not empty).
    /// </item>
    /// <item>
    /// <see cref="OffsetTypeNext"/>: starting from the next offset to be written.
    /// Contrary to OffsetTypeLast, consuming with OffsetTypeNext will not return anything if no-one is publishing to the stream.
    /// The broker will start sending messages to the consumer when messages are published to the stream.
    /// </item>
    /// <item>
    /// <see cref="OffsetTypeOffset"/>(offset): starting from the specified offset.
    /// 0 means consuming from the beginning of the stream (first messages).
    /// The client can also specify any number, for example the offset where it left off in a previous incarnation of the application.
    /// </item>
    /// <item>
    /// <see cref="OffsetTypeTimestamp"/>(timestamp): starting from the messages stored after the specified timestamp.
    /// </item>
    /// </list>
    /// </summary>
    public IOffsetType OffsetSpec { get; set; } = new OffsetTypeNext();

    /// <summary>
    /// When the single active consumer feature is enabled for several consumer instances sharing the same stream and name,
    /// only one of these instances will be active at a time and will receive messages.
    /// The other instances will be idle.
    /// </summary>
    public bool IsSingleActiveConsumer { get; set; }

    /// <summary>
    /// The broker notifies a consumer that becomes active or inactive. 
    /// With ConsumerUpdateListener the consumer, when active, can decide where to start consuming from.
    /// The event is raised only in case of single active consumer.
    /// The Code inside ConsumerUpdateListener _must_ be safe and should not throw exceptions.
    /// In case of exception, the library will use the default behavior that is to start consuming from OffsetNext().
    /// The code _must_ be fast since it runs in the
    /// socket thread, and it could impact the consumer promotion to Active.
    /// if null, the library will use the default behavior that is to start consuming from OffsetNext().
    /// </summary>
    public Func<string, string, bool, Task<IOffsetType>> ConsumerUpdateListener { get; set; }

    /// <summary>
    /// Initial credits for the consumer. If not set, the default value is 2.
    /// This is the number of chunks the consumer will receive at the beginning.
    /// A higher value can increase throughput but may increase memory usage and server-side CPU usage.
    /// The <see cref="RawConsumer"/> uses this value to create the channel buffer, so all chunks are stored in buffer memory.
    /// The default value is usually sufficient.
    /// </summary>
    public ushort InitialCredits { get; set; } = Consts.ConsumerInitialCredits;

    /// <summary>
    /// Filter enable the consumer to receive only the messages that match the filter.
    /// Filter.Values is the list of the values that the filter will match.
    /// Filter.PostFilter is the function that will be executed after the filter.
    /// The filter applied is a bloom filter, so there is a possibility of false positives.
    /// The PostFilter helps to avoid false positives.
    /// </summary>
    public ConsumerFilter Filter { get; set; } = null;

    /// <summary>
    /// Enable the check of the crc on the delivery when set to an implementation
    /// of <see cref="ICrc32"><code>ICrc32</code></see>.
    /// </summary>
    public ICrc32 Crc32 { get; set; } = new StreamCrc32();

    /// <summary>
    /// Controls the flow of the consumer. See <see cref="ConsumerFlowStrategy"/> for the available strategies.
    /// </summary>
    public FlowControl FlowControl { get; set; } = new FlowControl();

    /// <summary>
    /// Creates a new consumer configuration for the given stream system and stream.
    /// </summary>
    /// <param name="streamSystem">The stream system to consume from.</param>
    /// <param name="stream">The name of the stream (or super stream) to consume from.</param>
    public ConsumerConfig(StreamSystem streamSystem, string stream) : base(streamSystem, stream)
    {
    }
}

/// <summary>
/// Consumer is a wrapper around the standard <see cref="RawConsumer"/>.
/// Main features are:
/// <list type="bullet">
/// <item>
/// Auto-reconnection if the connection is dropped
/// </item>
/// <item>
/// Automatically restart consuming from the last offset 
/// </item>
/// <item>
/// Handle the Metadata Update. In case the stream is deleted Producer closes Producer/Connection.
/// </item>
/// <item>
/// Reconnect the Consumer if the stream still exists.
/// </item>
/// </list>
/// </summary>
public class Consumer : ConsumerFactory
{
    private readonly ILogger<Consumer> _logger;

    protected override ILogger BaseLogger => _logger;

    internal Consumer(ConsumerConfig consumerConfig, ILogger<Consumer> logger = null)
    {
        _logger = logger ?? NullLogger<Consumer>.Instance;
        _consumerConfig = consumerConfig;
    }

    /// <summary>
    /// Creates and initializes a reliable consumer with the given configuration.
    /// The consumer will auto-reconnect on connection loss and resume from the last consumed offset.
    /// </summary>
    /// <param name="consumerConfig">The consumer configuration. <see cref="ConsumerConfig.Reference"/> is mandatory
    /// to track the offset or to identify the consumer 
    /// <see cref="ConsumerConfig.MessageHandler"/> to handle the messages.</param>
    /// <param name="logger">Optional logger for diagnostics. If null, a null logger is used.</param>
    /// <returns>A fully initialized <see cref="Consumer"/> instance ready to receive messages.</returns>
    public static async Task<Consumer> Create(ConsumerConfig consumerConfig, ILogger<Consumer> logger = null)
    {
        consumerConfig.ReconnectStrategy ??= new BackOffReconnectStrategy(logger);
        consumerConfig.ResourceAvailableReconnectStrategy ??= new ResourceAvailableBackOffReconnectStrategy(logger);
        var rConsumer = new Consumer(consumerConfig, logger);
        await rConsumer.Init(consumerConfig)
            .ConfigureAwait(false);

        return rConsumer;
    }

    /// <summary>
    /// Creates or recreates the underlying consumer (standard or super stream) and establishes the connection.
    /// </summary>
    /// <param name="boot">True on first creation; false when reconnecting after a disconnect.</param>
    /// <returns>The info of the newly created consumer entity.</returns>
    protected override async Task<Info> CreateNewEntity(bool boot)
    {
        _consumer = await CreateConsumer(boot).ConfigureAwait(false);
        await _consumerConfig.ReconnectStrategy.WhenConnected(ToString()).ConfigureAwait(false);
        return _consumer.Info;
    }

    /// <summary>
    /// Closes the underlying raw consumer and releases resources. Used internally on close and on metadata updates.
    /// </summary>
    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_consumer != null)
            {
                await _consumer.Close().ConfigureAwait(false);
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Closes the consumer gracefully, stops receiving messages, and releases connections and resources.
    /// After closing, the consumer cannot be used again.
    /// </summary>
    public override async Task Close()
    {
        if (_status == ReliableEntityStatus.Initialization)
        {
            UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser, Info.Partitions);
            return;
        }

        UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser, Info.Partitions);
        await CloseEntity().ConfigureAwait(false);
        _logger?.LogDebug("Consumer {Identity} closed", ToString());
    }

    /// <summary>
    /// Returns a string representation of this consumer, including reference, stream, identifier, and client name.
    /// </summary>
    /// <returns>A string describing the consumer instance.</returns>
    public override string ToString()
    {
        return $"Consumer reference: {_consumerConfig.Reference}, " +
               $"stream: {_consumerConfig.Stream}, " +
               $"identifier: {_consumerConfig.Identifier}, " +
               $"client name: {_consumerConfig.ClientProvidedName} ";
    }

    /// <summary>
    /// Gets the consumer information (stream, reference, identifier, and partitions for super streams).
    /// </summary>
    public ConsumerInfo Info
    {
        get { return _consumer.Info; }
    }
}
