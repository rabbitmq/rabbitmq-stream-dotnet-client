// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

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
}

[AttributeUsage(AttributeTargets.Method)]
internal class MyMethodAttribute : Attribute
{
    public string Message { get; }

    public MyMethodAttribute(string message)
    {
        Message = message;
    }
}

public record ProducerConfig : ReliableConfig
{
    private readonly TimeSpan _timeoutMessageAfter = TimeSpan.FromSeconds(3);

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
    private IProducer _producer;
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

    /// <summary>
    /// Create a new Producer.
    /// <param name="producerConfig">Producer Configuration. Where StreamSystem and Stream are mandatory.</param>
    /// <param name="logger">Enable the logging. By default is null. Add a logging is suggested</param>
    /// </summary> 
    public static async Task<Producer> Create(ProducerConfig producerConfig, ILogger<Producer> logger = null)
    {
        producerConfig.ReconnectStrategy ??= new BackOffReconnectStrategy(logger);
        var rProducer = new Producer(producerConfig, logger);
        await rProducer.Init(producerConfig.ReconnectStrategy).ConfigureAwait(false);
        logger?.LogDebug(
            "Producer: {Reference} created for Stream: {Stream}",
            producerConfig.Reference,
            producerConfig.Stream
        );

        return rProducer;
    }

    internal override async Task CreateNewEntity(bool boot)
    {
        _producer = await CreateProducer().ConfigureAwait(false);

        await _producerConfig.ReconnectStrategy.WhenConnected(ToString()).ConfigureAwait(false);

        if (boot)
        {
            // Init the publishing id
            Interlocked.Exchange(ref _publishingId,
                await _producer.GetLastPublishingId().ConfigureAwait(false));

            // confirmation Pipe can start only if the producer is ready
            _confirmationPipe.Start();
        }
    }

    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync(Consts.LongWait).ConfigureAwait(false);
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

    public override async Task Close()
    {
        await SemaphoreSlim.WaitAsync(Consts.ShortWait).ConfigureAwait(false);
        try
        {
            _isOpen = false;
            _confirmationPipe.Stop();
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
    /// This is the standard way to send messages.
    /// The client aggregates the messages and sends them to the server in batches.
    /// The publisherId is automatically set.
    /// </summary>
    /// <param name="message">Standard Message</param>
    /// The method does not raise any exception during the send.
    /// In case of error the message is considered as timed out, you will receive a confirmation with the status TimedOut.
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
        _confirmationPipe.AddUnConfirmedMessage(publishingId, message);
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (!(_inReconnection))
            {
                await _producer.Send(publishingId, message).ConfigureAwait(false);
            }
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
    /// Enable sub-entry batch feature.
    /// It is needed when you need to sub aggregate the messages and compress them.
    /// For example you can aggregate 100 log messages and compress to reduce the space.
    /// One single publishingId can have multiple sub-batches messages.
    /// See also: https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#sub-entry-batching-and-compression
    /// </summary>
    /// <param name="messages">Messages to aggregate</param>
    /// <param name="compressionType"> Type of compression. By default the client supports GZIP and none</param>
    /// <returns></returns>
    /// The method does not raise any exception during the send.
    /// In case of error the messages are considered as timed out, you will receive a confirmation with the status TimedOut.
    public async ValueTask Send(List<Message> messages, CompressionType compressionType)
    {
        await SemaphoreSlim.WaitAsync().ConfigureAwait(false);
        Interlocked.Increment(ref _publishingId);
        _confirmationPipe.AddUnConfirmedMessage(_publishingId, messages);
        try
        {
            if (!_inReconnection)
            {
                await _producer.Send(_publishingId, messages, compressionType).ConfigureAwait(false);
            }
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

    public override string ToString()
    {
        return $"Producer reference: {_producerConfig.Reference}, stream: {_producerConfig.Stream} ";
    }

    /// <summary>
    /// Send the messages in batch to the stream in synchronous mode.
    /// The aggregation is provided by the user.
    /// The client will send the messages in the order they are provided.
    /// </summary>
    /// <param name="messages">Batch messages to send</param>
    /// <returns></returns>
    /// The method does not raise any exception during the send.
    /// In case of error the messages are considered as timed out, you will receive a confirmation with the status TimedOut.
    public async ValueTask Send(List<Message> messages)
    {
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
            if (!(_inReconnection))
            {
                await _producer.Send(messagesToSend).ConfigureAwait(false);
            }
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
}
