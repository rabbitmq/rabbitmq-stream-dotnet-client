// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client.Reliable;

public record SuperStreamConfig
{
    public bool Enabled { get; init; } = true;
    public Func<Message, string> Routing { get; set; }

    public RoutingStrategyType RoutingStrategyType { get; set; } = RoutingStrategyType.Hash;
}

public record ProducerConfig : ReliableConfig
{
    private readonly TimeSpan _timeoutMessageAfter = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Reference used for deduplication.
    /// For the Producer Class, it is not needed to set this value
    /// See DeduplicatingProducer for Deduplication Messages where this value is needed.
    /// </summary>
    internal string _reference;

    public string Reference
    {
        get { return _reference; }
        [Obsolete("Deprecated. Use ClientProvidedName instead. Se DeduplicatingProducer for Deduplication Messages ",
            false)]
        set { _reference = value; }
    }

    /// <summary>
    /// Publish confirmation callback.<br/>
    /// Used to inform publisher about success or failure of a publish.<br/>
    /// Depending on publishing function used, either single message or whole batch can be in MessagesConfirmation.<br/>
    /// For statuses that can be set on MessagesConfirmation, check <see cref="ConfirmationStatus"/>.
    /// </summary>
    public Func<MessagesConfirmation, Task> ConfirmationHandler { get; init; }

    /// <summary>
    /// The client name used to identify the producer. 
    /// You can see this value on the Management UI or in the connection details.
    /// </summary>

    public string ClientProvidedName { get; set; } = "dotnet-stream-producer";

    /// <summary>
    /// Function used to set the value of the filter<br/>
    /// The filter enable the server to filter the messages sent to the consumer.<br/>
    /// </summary>
    public ProducerFilter Filter { get; set; } = null;

    public int MaxInFlight { get; set; } = 1000;

    /// <summary>
    /// Number of the messages sent for each frame-send.<br/>
    /// High values can increase the throughput.<br/>
    /// Low values can reduce the messages latency.<br/>
    /// Default value is 100.
    /// </summary>
    public int MessagesBufferSize { get; set; } = 100;

    /// <summary>
    /// SuperStream configuration enables the SuperStream feature.
    /// </summary>
    public SuperStreamConfig SuperStreamConfig { get; set; }

    /// <summary>
    /// TimeSpan after which Producer should give up on published message(s).
    /// </summary>
    /// <exception cref="ValidationException">Thrown if value is unreasonably low.</exception>
    public TimeSpan TimeoutMessageAfter
    {
        get => _timeoutMessageAfter;
        init
        {
            if (value.TotalMilliseconds < 1000)
            {
                throw new ValidationException("TimeoutMessageAfter has to be at least 1000ms");
            }

            _timeoutMessageAfter = value;
        }
    }

    public ProducerConfig(StreamSystem streamSystem, string stream) : base(streamSystem, stream)
    {
    }
}

/// <summary>
/// Producer is a wrapper around the standard RawProducer/RawSuperStream Consumer.
/// Main features are:
/// <list type="bullet">
/// <item>
/// Auto-reconnection if the connection is dropped
/// </item>
/// <item>
/// Trace sent and received messages.
/// <see cref="ProducerConfig.ConfirmationHandler"/> will be invoked whenever message is (n)acked. 
/// </item>
/// <item>
/// Handle the Metadata Update.
/// In case the stream is deleted Producer closes Producer/Connection.
/// In case stream still exists, reconnect the producer.
/// </item>
/// Automatically set the next PublisherId.
/// <item>
/// </item>
/// <item>
/// Automatically retrieves the last sequence.
/// </item>
/// </list>
/// </summary>
public class Producer : ProducerFactory
{
    private ulong _publishingId;
    private readonly ILogger<Producer> _logger;

    protected override ILogger BaseLogger => _logger;

    private Producer(ProducerConfig producerConfig, ILogger<Producer> logger = null)
    {
        _producerConfig = producerConfig;
        _confirmationPipe = new ConfirmationPipe(
            producerConfig.ConfirmationHandler,
            producerConfig.TimeoutMessageAfter,
            producerConfig.MaxInFlight
        );
        _logger = logger ?? NullLogger<Producer>.Instance;
    }

    private void ThrowIfClosed()
    {
        if (!IsOpen())
        {
            throw new AlreadyClosedException("Producer is closed");
        }
    }

    /// <summary>
    /// Creates a new Producer.
    /// </summary>
    /// <param name="producerConfig">Producer configuration. StreamSystem and Stream are mandatory.</param>
    /// <param name="logger">Optional logger for producer events. Logging is recommended for production.</param>
    /// <returns>A task that completes with the created Producer instance.</returns>
    public static async Task<Producer> Create(ProducerConfig producerConfig, ILogger<Producer> logger = null)
    {
        producerConfig.ReconnectStrategy ??= new BackOffReconnectStrategy(logger);
        producerConfig.ResourceAvailableReconnectStrategy ??= new ResourceAvailableBackOffReconnectStrategy(logger);
        var rProducer = new Producer(producerConfig, logger);
        await rProducer.Init(producerConfig)
            .ConfigureAwait(false);
        return rProducer;
    }

    protected override async Task<Info> CreateNewEntity(bool boot)
    {
        _producer = await CreateProducer(boot).ConfigureAwait(false);

        await _producerConfig.ReconnectStrategy.WhenConnected(ToString()).ConfigureAwait(false);

        if (boot)
        {
            // Init the publishing id
            Interlocked.Exchange(ref _publishingId,
                await _producer.GetLastPublishingId().ConfigureAwait(false));

            // confirmation Pipe can start only if the producer is ready
            _confirmationPipe.Start();
        }

        return Info;
    }

    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_producer != null)
            {
                await _producer.Close().ConfigureAwait(false);
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Closes the producer and releases all associated resources.
    /// Stops the confirmation pipe and closes the underlying connection to the broker.
    /// Call this method when the producer is no longer needed (e.g. during application shutdown).
    /// </summary>
    public override async Task Close()
    {
        if (ReliableEntityStatus.Initialization == _status)
        {
            UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser, Info.Partitions);
            return;
        }

        UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser, Info.Partitions);
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            await _confirmationPipe.StopAsync().ConfigureAwait(false);
            if (_producer != null)
            {
                await _producer.Close().ConfigureAwait(false);
                _logger?.LogDebug("Producer closed for {Stream}", _producerConfig.Stream);
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Sends a single message to the stream.
    /// The client aggregates messages and sends them to the server in batches; the publisher ID is set automatically.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <remarks>
    /// This method does not throw during send. On error the message is treated as timed out;
    /// <see cref="ProducerConfig.ConfirmationHandler"/> will be invoked with status <see cref="ConfirmationStatus.TimedOut"/>.
    /// </remarks>
    public async ValueTask Send(Message message)
    {
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            await SendInternal(Interlocked.Increment(ref _publishingId), message).ConfigureAwait(false);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    internal async Task<ulong> GetLastPublishingId()
    {
        return await _producer.GetLastPublishingId().ConfigureAwait(false);
    }

    internal async ValueTask SendInternal(ulong publishingId, Message message)
    {
        ThrowIfClosed();
        _confirmationPipe.AddUnConfirmedMessage(publishingId, message);
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (_producer.IsOpen())
            {
                await _producer.Send(publishingId, message).ConfigureAwait(false);
            }
            else
            {
                _logger?.LogDebug("The internal producer is closed. Message will be timed out");
            }
        }

        // see the RouteNotFoundException comment
        catch (RouteNotFoundException)
        {
            throw;
        }

        catch (Exception e)
        {
            _logger?.LogError(e, "Error sending message. " +
                                 "Most likely the message is not sent to the stream: {Stream}." +
                                 "Message wont' receive confirmation so you will receive a timeout error",
                _producerConfig.Stream);
        }
    }

    /// <summary>
    /// Sends a batch of messages using sub-entry batching and compression.
    /// Use this when aggregating many messages (e.g. logs) and compressing to save space.
    /// A single publishing ID can contain multiple sub-batches.
    /// See also: https://rabbitmq.github.io/rabbitmq-stream-dotnet-client/stable/htmlsingle/index.html#sub-entry-batching-and-compressionsummary>
    /// <param name="messages">The messages to aggregate and send.</param>
    /// <param name="compressionType">Compression type (e.g. GZIP or None).</param>
    /// <remarks>
    /// This method does not throw during send. On error the batch is treated as timed out;
    /// <see cref="ProducerConfig.ConfirmationHandler"/> will be invoked with status <see cref="ConfirmationStatus.ClientTimeoutError"/>.
    /// </remarks>
    public async ValueTask Send(List<Message> messages, CompressionType compressionType)
    {
        ThrowIfClosed();
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _publishingId);
        _confirmationPipe.AddUnConfirmedMessage(_publishingId, messages);
        try
        {
            if (_producer.IsOpen())
            {
                await _producer.Send(_publishingId, messages, compressionType).ConfigureAwait(false);
            }
            else
            {
                _logger?.LogDebug("The internal producer is closed. Message will be timed out");
            }
        }

        // see the RouteNotFoundException comment
        catch (RouteNotFoundException)
        {
            throw;
        }

        catch (Exception e)
        {
            _logger?.LogError(e, "Error sending sub-batch messages. " +
                                 "Most likely the messages are not sent to the stream: {Stream}." +
                                 " Messages wont' receive confirmation so you will receive a timeout error",
                _producerConfig.Stream);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Returns a string representation of the producer including stream name, identifier, and client name.
    /// </summary>
    /// <returns>A string describing the producer instance.</returns>
    public override string ToString()
    {
        return $"Producer stream: {_producerConfig.Stream}, " +
               $"identifier: {_producerConfig.Identifier}, " +
               $"client name: {_producerConfig.ClientProvidedName}";
    }

    /// <summary>
    /// Sends a batch of messages to the stream.
    /// The caller provides the batch; messages are sent in the given order with no additional buffering.
    /// </summary>
    /// <param name="messages">The batch of messages to send.</param>
    /// <remarks>
    /// This method does not throw during send. On error the messages are treated as timed out;
    /// <see cref="ProducerConfig.ConfirmationHandler"/> will be invoked with status <see cref="ConfirmationStatus.ClientTimeoutError"/>.
    /// </remarks>
    public async ValueTask Send(List<Message> messages)
    {
        ThrowIfClosed();
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        var messagesToSend = new List<(ulong, Message)>();
        foreach (var message in messages)
        {
            Interlocked.Increment(ref _publishingId);
            messagesToSend.Add((_publishingId, message));
        }

        foreach (var msg in messagesToSend)
        {
            _confirmationPipe.AddUnConfirmedMessage(msg.Item1, msg.Item2);
        }

        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (_producer.IsOpen())
            {
                await _producer.Send(messagesToSend).ConfigureAwait(false);
            }
            else
            {
                _logger?.LogDebug("The internal producer is closed. Message will be timed out");
            }
        }

        // see the RouteNotFoundException comment
        catch (RouteNotFoundException)
        {
            throw;
        }

        catch (Exception e)
        {
            _logger?.LogError(e, "Error sending messages. " +
                                 "Most likely the messages are not sent to the stream: {Stream}." +
                                 "Messages wont' receive confirmation so you will receive a timeout error",
                _producerConfig.Stream);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Gets the producer information including stream name and connection details.
    /// Available after the producer has been successfully created.
    /// </summary>
    public ProducerInfo Info { get { return _producer.Info; } }
}
