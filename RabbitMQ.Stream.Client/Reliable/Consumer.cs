// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

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
    /// The broker notifies a consumer that becomes active before dispatching messages to it. 
    /// With ConsumerUpdateListener the consumer can decide where to start consuming from.
    /// The event is raised only in case of single active consumer.
    /// </summary>
    public Func<string, string, bool, Task<IOffsetType>> ConsumerUpdateListener { get; set; }

    // InitialCredits is the initial credits to be used for the consumer.
    // if the InitialCredits is not set, the default value will be 2.
    // It is the number of the chunks that the consumer will receive at beginning.
    // A high value can increase the throughput but could increase the memory usage and server-side CPU usage.
    // The RawConsumer uses this value to create the Channel buffer so all the chunks will be stored in the buffer memory.
    // The default value it is usually a good value.
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
    /// The server will send the crc for each chunk and the client will check it.
    /// It is not enabled by default. In some case it is could reduce the performance.
    /// ICrc32 is an interface that can be implemented by the user with the desired implementation.
    /// The client is tested with the System.IO.Hashing.Crc32 implementation, like:
    ///<c>
    /// private class UserCrc32 : ICrc32 
    /// {
    /// public byte[] Hash(byte[] data)
    /// {
    /// return System.IO.Hashing.Crc32.Hash(data);
    /// }
    /// }
    /// </c>
    /// </summary>
    public ICrc32 Crc32 { get; set; } = null;

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
    private IConsumer _consumer;
    private readonly ILogger<Consumer> _logger;

    protected override ILogger BaseLogger => _logger;

    internal Consumer(ConsumerConfig consumerConfig, ILogger<Consumer> logger = null)
    {
        _logger = logger ?? NullLogger<Consumer>.Instance;
        _consumerConfig = consumerConfig;
        Info = new ConsumerInfo(consumerConfig.Stream, consumerConfig.Reference,consumerConfig.ClientProvidedName);
    }

    public static async Task<Consumer> Create(ConsumerConfig consumerConfig, ILogger<Consumer> logger = null)
    {
        consumerConfig.ReconnectStrategy ??= new BackOffReconnectStrategy(logger);
        var rConsumer = new Consumer(consumerConfig, logger);
        await rConsumer.Init(consumerConfig.ReconnectStrategy).ConfigureAwait(false);
        logger?.LogDebug("Consumer: {Reference} created for Stream: {Stream}",
            consumerConfig.Reference, consumerConfig.Stream);

        return rConsumer;
    }

    internal override async Task CreateNewEntity(bool boot)
    {
        _consumer = await CreateConsumer(boot).ConfigureAwait(false);
        await _consumerConfig.ReconnectStrategy.WhenConnected(ToString()).ConfigureAwait(false);
    }

    // just close the consumer. See base/metadataupdate
    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync(Consts.LongWait).ConfigureAwait(false);
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

    public override async Task Close()
    {
        _isOpen = false;
        await CloseEntity().ConfigureAwait(false);
        _logger?.LogDebug("Consumer closed for stream {Stream}", _consumerConfig.Stream);
    }

    public override string ToString()
    {
        return $"Consumer reference: {_consumerConfig.Reference}, stream: {_consumerConfig.Stream} ";
    }

    public ConsumerInfo Info { get; }
}
