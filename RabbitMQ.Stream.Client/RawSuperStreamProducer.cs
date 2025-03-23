// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
public class RawSuperStreamProducer : ISuperStreamProducer, IDisposable
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
    private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

    public static ISuperStreamProducer Create(
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
        ClientParameters clientParameters, ILogger logger = null
    )
    {
        _config = config;
        _streamInfos = streamInfos;
        _clientParameters = clientParameters;
        Info = new ProducerInfo(config.SuperStream, config.Reference, config.Identifier, streamInfos.Keys.ToList());
        _defaultRoutingConfiguration.RoutingStrategy = _config.RoutingStrategyType switch
        {
            RoutingStrategyType.Key => new KeyRoutingStrategy(_config.Routing,
                _config.Client.QueryRoute, _config.SuperStream),
            RoutingStrategyType.Hash => new HashRoutingMurmurStrategy(_config.Routing),
            _ => new HashRoutingMurmurStrategy(_config.Routing)
        };
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
            Filter = _config.Filter,
            Pool = _config.Pool,
            Identifier = _config.Identifier,
            ConnectionClosedHandler = async (reason) =>
            {
                _producers.TryGetValue(stream, out var producer);
                if (reason == ConnectionClosedReason.Normal)
                {
                    _logger.LogDebug("Super Stream producer {@ProducerInfo} is closed normally", producer?.Info);
                }
                else
                {
                    _logger.LogWarning(
                        "Super Stream producer {@ProducerInfo} is disconnected from {StreamIdentifier} reason: {Reason}",
                        producer?.Info,
                        stream, reason
                    );
                }

                if (_config.ConnectionClosedHandler != null)
                {
                    await _config.ConnectionClosedHandler(reason, stream).ConfigureAwait(false);
                }
            },
            MetadataHandler = async update =>
            {
                if (_config.MetadataHandler != null)
                {
                    await _config.MetadataHandler(update).ConfigureAwait(false);
                }
            },
            ClientProvidedName = _config.ClientProvidedName,
            BatchSize = _config.BatchSize,
            MessagesBufferSize = _config.MessagesBufferSize,
        };
    }

    // The producer is created on demand when a message is sent to a stream
    private async Task<IProducer> InitProducer(string stream)
    {
        var index = _streamInfos.Keys.Select((item, index) => new { Item = item, Index = index })
            .First(i => i.Item == stream).Index;
        var p = await RawProducer.Create(
                _clientParameters with { ClientProvidedName = $"{_config.ClientProvidedName}_{index}" },
                FromStreamConfig(stream),
                _streamInfos[stream],
                _logger)
            .ConfigureAwait(false);
        _logger?.LogDebug("Super stream producer {@ProducerReference} created for Stream {StreamIdentifier}", p.Info,
            stream);
        return p;
    }

    private void ThrowIfClosed()
    {
        if (!IsOpen())
        {
            throw new AlreadyClosedException($"Super stream {_config.SuperStream} is closed.");
        }
    }

    private async Task<IProducer> MaybeAddAndGetProducer(string stream)
    {
        if (!_producers.ContainsKey(stream))
        {
            var p = await InitProducer(stream).ConfigureAwait(false);
            _producers.TryAdd(stream, p);
        }

        return _producers[stream];
    }

    public async Task ReconnectPartition(StreamInfo streamInfo)
    {
        ClientExceptions.CheckLeader(streamInfo);
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            _producers.TryRemove(streamInfo.Stream, out var producer);
            producer?.Close();
            _streamInfos[streamInfo.Stream] = streamInfo; // add the new stream infos
            await MaybeAddAndGetProducer(streamInfo.Stream).ConfigureAwait(false);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    // based on the stream name and the partition key, we select the producer
    private async Task<IProducer> GetProducerForMessage(Message message)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RawSuperStreamProducer));
        }

        var routes = await _defaultRoutingConfiguration.RoutingStrategy.Route(message,
            _streamInfos.Keys.ToList()).ConfigureAwait(false);

        // we should always have a route
        // but in case of stream KEY the routing could not exist
        if (routes is not { Count: > 0 })
        {
            throw new RouteNotFoundException("No route found for the message to any stream");
        }

        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            return await MaybeAddAndGetProducer(routes[0]).ConfigureAwait(false);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async ValueTask Send(ulong publishingId, Message message)
    {
        ThrowIfClosed();
        var producer = await GetProducerForMessage(message).ConfigureAwait(false);
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            await producer.Send(publishingId, message).ConfigureAwait(false);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async ValueTask Send(List<(ulong, Message)> messages)
    {
        ThrowIfClosed();
        var aggregate = new List<(IProducer, List<(ulong, Message)>)>();

        // this part is not super-optimized
        // we have to re-assemble the messages in the right order
        // and send them to the right producer
        foreach (var subMessage in messages)
        {
            var p = await GetProducerForMessage(subMessage.Item2).ConfigureAwait(false);
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

        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            foreach (var (producer, list) in aggregate)
            {
                await producer.Send(list).ConfigureAwait(false);
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
    {
        ThrowIfClosed();
        var aggregate = new List<(IProducer, List<Message>)>();

        // this part is not super-optimized
        // we have to re-assemble the messages in the right order
        // and send them to the right producer
        foreach (var subMessage in subEntryMessages)
        {
            var p = await GetProducerForMessage(subMessage).ConfigureAwait(false);
            if (aggregate.Any(a => a.Item1 == p))
            {
                aggregate.First(a => a.Item1 == p).Item2.Add(subMessage);
            }
            else
            {
                aggregate.Add((p, new List<Message>() { subMessage }));
            }
        }

        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            // Here we send the messages to the right producer
            // sub aggregate is a list of messages that have to be sent to the same producer
            foreach (var (producer, messages) in aggregate)
            {
                await producer.Send(publishingId, messages, compressionType).ConfigureAwait(false);
            }
        }
        finally
        {
            _semaphoreSlim.Release();
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
    public async Task<ulong> GetLastPublishingId()
    {
        foreach (var stream in _streamInfos.Keys.ToList())
        {
            await MaybeAddAndGetProducer(stream).ConfigureAwait(false);
        }

        var v = _producers.Values.Min(p => p.GetLastPublishingId().Result);

        return v;
    }

    public bool IsOpen()
    {
        return !_disposed;
    }

    public void Dispose()
    {
        foreach (var (_, iProducer) in _producers)
        {
            iProducer.Dispose();
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    public int MessagesSent => _producers.Sum(x => x.Value.MessagesSent);
    public int ConfirmFrames => _producers.Sum(x => x.Value.ConfirmFrames);
    public int IncomingFrames => _producers.Sum(x => x.Value.IncomingFrames);
    public int PublishCommandsSent => _producers.Sum(x => x.Value.PublishCommandsSent);
    public int PendingCount => _producers.Sum(x => x.Value.PendingCount);
    public ProducerInfo Info { get; }
}

public enum RoutingStrategyType
{
    Hash,
    Key,
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

    public RoutingStrategyType RoutingStrategyType { get; set; } = RoutingStrategyType.Hash;

    public Func<string, string, Task> ConnectionClosedHandler { get; set; }

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
    Task<List<string>> Route(Message message, List<string> partitions);
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
    public Task<List<string>> Route(Message message, List<string> partitions)
    {
        var key = _routingKeyExtractor(message);
        var hash = new Murmur32ManagedX86(Seed).ComputeHash(Encoding.UTF8.GetBytes(key));
        var index = BitConverter.ToUInt32(hash, 0) % (uint)partitions.Count;
        var r = new List<string>() { partitions[(int)index] };
        return Task.FromResult(r);
    }

    public HashRoutingMurmurStrategy(Func<Message, string> routingKeyExtractor)
    {
        _routingKeyExtractor = routingKeyExtractor;
    }
}

/// <summary>
/// KeyRoutingStrategy is a routing strategy that uses the routing key to route messages to streams.
/// </summary>
public class KeyRoutingStrategy : IRoutingStrategy
{
    private readonly Func<Message, string> _routingKeyExtractor;
    private readonly Func<string, string, Task<RouteQueryResponse>> _routingKeyQFunc;
    private readonly string _superStream;
    private readonly Dictionary<string, List<string>> _cacheStream = new();

    public async Task<List<string>> Route(Message message, List<string> partitions)
    {
        var key = _routingKeyExtractor(message);
        // If the stream is already in the cache we return it
        // to avoid a query to the server for each send
        if (_cacheStream.TryGetValue(key, out var value))
        {
            return value;
        }

        var c = await _routingKeyQFunc(_superStream, key).ConfigureAwait(false);
        _cacheStream[key] = c.Streams;
        return (from resultStream in c.Streams
                where partitions.Contains(resultStream)
                select new List<string>() { resultStream }).FirstOrDefault();
    }

    public KeyRoutingStrategy(Func<Message, string> routingKeyExtractor,
        Func<string, string, Task<RouteQueryResponse>> routingKeyQFunc, string superStream)
    {
        _routingKeyExtractor = routingKeyExtractor;
        _routingKeyQFunc = routingKeyQFunc;
        _superStream = superStream;
    }
}
