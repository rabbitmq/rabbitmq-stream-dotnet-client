// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

public record SuperStreamConfig()
{
    public bool Enabled { get; init; } = true;
    public Func<Message, string> Routing { get; set; } = null;
}

public record ProducerConfig : ReliableConfig
{
    private readonly TimeSpan _timeoutMessageAfter = TimeSpan.FromSeconds(3);

    // <summary>
    // Reference is mostly used for deduplication. In most of the cases reference is not needed.
    // </summary>
    public string Reference { get; set; }

    // <summary>
    // Confirmation is used to confirm that the message has been received by the server.
    // After the timeout TimeoutMessageAfter/0 the message is considered not confirmed.
    // See MessagesConfirmation.ConfirmationStatus for more details.
    // </summary>
    public Func<MessagesConfirmation, Task> ConfirmationHandler { get; init; }

    // <summary>
    // The client name used to identify the producer. 
    // You can see this value on the Management UI or in the connection detail
    // </summary>

    public string ClientProvidedName { get; set; } = "dotnet-stream-producer";

    public int MaxInFlight { get; set; } = 1000;

    // SuperStream configuration enables the SuperStream feature
    public SuperStreamConfig SuperStreamConfig { get; set; } = null;

    // TimeoutMessageAfter is the time after which a message is considered as timed out
    // If client does not receive a confirmation for a message after this time, the message is considered as timed out
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
/// - Auto-reconnection if the connection is dropped
/// - Trace sent and received messages. The event Producer:ConfirmationHandler/2
///   receives back messages sent with the status.
/// - Handle the Metadata Update. In case the stream is deleted Producer closes Producer/Connection.
///   Reconnect the Producer if the stream still exists.
/// - Set automatically the next PublisherID
/// - Automatically retrieves the last sequence. By default is AutoPublishingId see IPublishingIdStrategy.
/// </summary>
public class Producer : ProducerFactory
{
    private IProducer _producer;
    private ulong _publishingId;

    private Producer(ProducerConfig producerConfig)
    {
        _producerConfig = producerConfig;
        _confirmationPipe = new ConfirmationPipe(
            producerConfig.ConfirmationHandler,
            producerConfig.TimeoutMessageAfter,
            producerConfig.MaxInFlight);
    }

    // <summary>
    // Create a new Producer
    // </summary> 
    public static async Task<Producer> Create(ProducerConfig producerConfig)
    {
        var rProducer = new Producer(producerConfig);
        await rProducer.Init(producerConfig.ReconnectStrategy);
        return rProducer;
    }

    internal override async Task CreateNewEntity(bool boot)
    {
        _producer = await CreateProducer();

        await _producerConfig.ReconnectStrategy.WhenConnected(ToString());

        if (boot)
        {
            // Init the publishing id
            Interlocked.Exchange(ref _publishingId,
                await _producer.GetLastPublishingId());

            // confirmation Pipe can start only if the producer is ready
            _confirmationPipe.Start();
        }
    }

    protected override async Task CloseEntity()
    {
        await SemaphoreSlim.WaitAsync(10);
        try
        {
            if (_producer != null)
            {
                await _producer.Close();
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    public override async Task Close()
    {
        await SemaphoreSlim.WaitAsync(TimeSpan.FromMilliseconds(10));
        try
        {
            _isOpen = false;
            _confirmationPipe.Stop();
            if (_producer != null)
            {
                await _producer.Close();
            }
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Send a message to the stream.
    /// The client aggregates the messages and sends them to the server in batches.
    /// The publisherId is automatically set. 
    /// </summary>
    /// <param name="message">Standard Message</param>
    public async ValueTask Send(Message message)
    {
        Interlocked.Increment(ref _publishingId);
        _confirmationPipe.AddUnConfirmedMessage(_publishingId, message);
        await SemaphoreSlim.WaitAsync();
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (!(_inReconnection))
            {
                await _producer.Send(_publishingId, message);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError("Error sending message: ", e);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Enable sub-batch feature.
    /// It is needed when you need to sub aggregate the messages and compress them.
    /// For example you can aggregate 100 log messages and compress to reduce the space.
    /// One single publishingId can have multiple sub-batches messages.
    /// See also: https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#sub-entry-batching-and-compression
    /// </summary>
    /// <param name="messages">Messages to aggregate</param>
    /// <param name="compressionType"> Type of compression. By default the client supports GZIP and none</param>
    /// <returns></returns>
    public async ValueTask Send(List<Message> messages, CompressionType compressionType)
    {
        Interlocked.Increment(ref _publishingId);
        _confirmationPipe.AddUnConfirmedMessage(_publishingId, messages);
        await SemaphoreSlim.WaitAsync();
        try
        {
            if (!_inReconnection)
            {
                await _producer.Send(_publishingId, messages, compressionType);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError("Error sending messages: ", e);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Send the messages in batch to the stream in synchronous mode.
    /// The aggregation is provided by the user.
    /// The client will send the messages in the order they are provided.
    /// </summary>
    /// <param name="messages">Batch messages to send</param>
    /// <returns></returns>
    public async ValueTask Send(List<Message> messages)
    {
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

        await SemaphoreSlim.WaitAsync();
        try
        {
            // This flags avoid some race condition,
            // since the reconnection can arrive from different threads. 
            // In this case it skips the publish until
            // the producer is connected. Messages are safe since are stored 
            // on the _waitForConfirmation list. The user will get Timeout Error
            if (!(_inReconnection))
            {
                await _producer.Send(messagesToSend);
            }
        }

        catch (Exception e)
        {
            LogEventSource.Log.LogError("BatchSend error sending message: ", e);
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
}
