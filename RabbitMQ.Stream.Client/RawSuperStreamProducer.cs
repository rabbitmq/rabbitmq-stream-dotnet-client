﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client.Hash;

namespace RabbitMQ.Stream.Client;

/// <summary>
/// RawSuperStreamProducer is a producer that can send messages to multiple streams.
/// Super Stream is available in RabbitMQ 3.11.0 and later.
/// See https://rabbitmq.com/streams.html#super-streams for more information.
///
/// When a message is sent to a stream, the producer will be selected based on the stream name and the partition key.
/// SuperStreamProducer uses lazy initialization for the producers, when it starts there are no producers until the first message is sent.
/// </summary>
public class RawSuperStreamProducer : IProducer, IDisposable
{
    private bool _disposed;

    // ConcurrentDictionary because the producer can be closed from another thread
    // The send operations will check if the producer exists and if not it will be created
    private readonly ConcurrentDictionary<string, IProducer> _producers = new();
    private readonly RawSuperStreamProducerConfig _config;

    private readonly DefaultRoutingConfiguration _defaultRoutingConfiguration = new();

    // streams are the streams created by the super stream
    // The partitioned streams.
    // For example:
    // invoices(super_stream) -> invoices-0, invoices-1, invoices-2
    // Streams contains the configuration for each stream but not the connection
    private readonly IDictionary<string, StreamInfo> _streamInfos;
    private readonly ClientParameters _clientParameters;
    private readonly ILogger _logger;

    public static IProducer Create(
        RawSuperStreamProducerConfig rawSuperStreamProducerConfig,
        IDictionary<string, StreamInfo> streamInfos,
        ClientParameters clientParameters,
        ILogger logger = null
    )
    {
        return new RawSuperStreamProducer(rawSuperStreamProducerConfig, streamInfos, clientParameters, logger);
    }

    private RawSuperStreamProducer(
        RawSuperStreamProducerConfig config,
        IDictionary<string, StreamInfo> streamInfos,
        ClientParameters clientParameters,
        ILogger logger = null
    )
    {
        _config = config;
        _streamInfos = streamInfos;
        _clientParameters = clientParameters;
        _defaultRoutingConfiguration.RoutingStrategy = new HashRoutingMurmurStrategy(_config.Routing);
        _logger = logger ?? NullLogger<RawSuperStreamProducer>.Instance;
    }

    // We need to copy the config from the super producer to the standard producer
    private RawProducerConfig FromStreamConfig(string stream)
    {
        return new RawProducerConfig(stream)
        {
            ConfirmHandler = confirmation =>
            {
                // The confirmation handler is routed to the super stream confirmation handler
                // The user doesn't see the confirmation for the each partitioned stream
                // but the global confirmation for the super stream
                _config.ConfirmHandler?.Invoke((stream, confirmation));
            },
            Reference = _config.Reference,
            MaxInFlight = _config.MaxInFlight,
            ConnectionClosedHandler = s =>
            {
                // In case of connection closed, we need to remove the producer from the list
                // We hide the behavior of the producer to the user
                // if needed the connection will be created again
                _producers.TryRemove(stream, out _);
                return Task.CompletedTask;
            },
            MetadataHandler = update =>
            {
                // In case of stream update we remove the producer from the list
                // We hide the behavior of the producer to the user
                // if needed the connection will be created again
                // we "should" always have the stream

                // but we have to handle the case¬
                // We need to wait a bit it can take some time to update the configuration
                Thread.Sleep(500);

                var exists = _config.Client.StreamExists(update.Stream);
                if (!exists.Result)
                {
                    // The stream doesn't exist anymore
                    // but this condition should be avoided since the hash routing 
                    // can be compromised
                    _logger.LogWarning("Stream {StreamIdentifier} is not available anymore", update.Stream);
                    _streamInfos.Remove(update.Stream);
                }

                _producers.TryRemove(update.Stream, out var producer);
                producer?.Close();
            },
            ClientProvidedName = _config.ClientProvidedName,
            BatchSize = _config.BatchSize,
            MessagesBufferSize = _config.MessagesBufferSize,
        };
    }

    // The producer is created on demand when a message is sent to a stream
    private async Task<IProducer> InitProducer(string stream)
    {
        var p = await RawProducer.Create(_clientParameters, FromStreamConfig(stream), _streamInfos[stream], _logger);
        _logger?.LogDebug("Producer {ProducerReference} created for Stream {StreamIdentifier}", _config.Reference,
            stream);
        return p;
    }

    private async Task<IProducer> GetProducer(string stream)
    {
        if (!_producers.ContainsKey(stream))
        {
            var p = await InitProducer(stream);
            _producers.TryAdd(stream, p);
        }

        return _producers[stream];
    }

    // based on the stream name and the partition key, we select the producer
    private async Task<IProducer> GetProducerForMessage(Message message)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RawSuperStreamProducer));
        }

        var routes = _defaultRoutingConfiguration.RoutingStrategy.Route(message,
            _streamInfos.Keys.ToList());
        return await GetProducer(routes[0]);
    }

    public async ValueTask Send(ulong publishingId, Message message)
    {
        var producer = await GetProducerForMessage(message);
        await producer.Send(publishingId, message);
    }

    public async ValueTask Send(List<(ulong, Message)> messages)
    {
        var aggregate = new List<(IProducer, List<(ulong, Message)>)>();

        // this part is not super-optimized
        // we have to re-assemble the messages in the right order
        // and send them to the right producer
        foreach (var subMessage in messages)
        {
            var p = await GetProducerForMessage(subMessage.Item2);
            if (aggregate.Any(a => a.Item1 == p))
            {
                aggregate.First(a => a.Item1 == p).Item2.Add((subMessage.Item1,
                    subMessage.Item2));
            }
            else
            {
                aggregate.Add((p, new List<(ulong, Message)>() { (subMessage.Item1, subMessage.Item2) }));
            }
        }

        foreach (var (producer, list) in aggregate)
        {
            await producer.Send(list);
        }
    }

    public async ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
    {
        var aggregate = new List<(IProducer, List<Message>)>();

        // this part is not super-optimized
        // we have to re-assemble the messages in the right order
        // and send them to the right producer
        foreach (var subMessage in subEntryMessages)
        {
            var p = await GetProducerForMessage(subMessage);
            if (aggregate.Any(a => a.Item1 == p))
            {
                aggregate.First(a => a.Item1 == p).Item2.Add(subMessage);
            }
            else
            {
                aggregate.Add((p, new List<Message>() { subMessage }));
            }
        }

        // Here we send the messages to the right producer
        // sub aggregate is a list of messages that have to be sent to the same producer
        foreach (var (producer, messages) in aggregate)
        {
            await producer.Send(publishingId, messages, compressionType);
        }
    }

    public Task<ResponseCode> Close()
    {
        if (_disposed)
        {
            Task.FromResult(ResponseCode.Ok);
        }

        Dispose();
        _logger?.LogDebug("Super stream Producer {ProducerReference} closed", _config.Reference);
        return Task.FromResult(ResponseCode.Ok);
    }

    //<summary>
    /// Returns lower from the LastPublishingId for all the producers
    // </summary> 
    public Task<ulong> GetLastPublishingId()
    {
        foreach (var stream in _streamInfos.Keys.ToList())
        {
            GetProducer(stream).Wait();
        }

        var v = _producers.Values.Min(p => p.GetLastPublishingId().Result);

        return Task.FromResult(v);
    }

    public bool IsOpen()
    {
        return !_disposed;
    }

    public void Dispose()
    {
        foreach (var (_, iProducer) in _producers)
        {
            iProducer.Close();
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    public int MessagesSent => _producers.Sum(x => x.Value.MessagesSent);
    public int ConfirmFrames => _producers.Sum(x => x.Value.ConfirmFrames);
    public int IncomingFrames => _producers.Sum(x => x.Value.IncomingFrames);
    public int PublishCommandsSent => _producers.Sum(x => x.Value.PublishCommandsSent);
    public int PendingCount => _producers.Sum(x => x.Value.PendingCount);
}

public record RawSuperStreamProducerConfig : IProducerConfig
{
    public RawSuperStreamProducerConfig(string superStream)
    {
        if (string.IsNullOrWhiteSpace(superStream))
        {
            throw new ArgumentException("SuperStream name cannot be null or empty", nameof(superStream));
        }

        SuperStream = superStream;
    }

    // SuperStreamName. The user interacts with this it
    // In AMQP this is the exchange name
    public string SuperStream { get; }

    // The routing key extractor is used to extract the routing key from the message
    // The routing key is used to route the message to a stream
    // The user _must_ provides a custom extractor
    public Func<Message, string> Routing { get; set; } = null;
    public Action<(string, Confirmation)> ConfirmHandler { get; set; } = _ => { };

    internal Client Client { get; set; }
}

public interface IRoutingConfiguration
{
}

/// <summary>
///  DefaultRoutingConfiguration is the default routing configuration
/// </summary>
internal class DefaultRoutingConfiguration : IRoutingConfiguration
{
    public IRoutingStrategy RoutingStrategy { get; internal set; }
}

/// <summary>
/// IRoutingStrategy the base interface for routing strategies
/// based on the message and the list of streams the traffic is routed to the stream
/// </summary>
public interface IRoutingStrategy
{
    List<string> Route(Message message, List<string> partitions);
}

/// <summary>
/// HashRoutingMurmurStrategy is a routing strategy that uses the Murmur hash
/// function to route messages to streams.
/// See the HASH/Murmur32ManagedX86 implementation in the RabbitMQ .NET client.
/// </summary>
public class HashRoutingMurmurStrategy : IRoutingStrategy
{
    // The routing key extractor is a function that extracts the routing key from the message
    // it is usually passed by the user
    private readonly Func<Message, string> _routingKeyExtractor;
    private const int Seed = 104729; //  must be the same to all the clients to be compatible

    // Routing based on the Murmur hash function
    public List<string> Route(Message message, List<string> partitions)
    {
        var key = _routingKeyExtractor(message);
        var hash = new Murmur32ManagedX86(Seed).ComputeHash(Encoding.UTF8.GetBytes(key));
        var index = BitConverter.ToUInt32(hash, 0) % (uint)partitions.Count;
        return new List<string>() { partitions[(int)index] };
    }

    public HashRoutingMurmurStrategy(Func<Message, string> routingKeyExtractor)
    {
        _routingKeyExtractor = routingKeyExtractor;
    }
}
