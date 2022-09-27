// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client.Hash;

namespace RabbitMQ.Stream.Client;

public class SuperStreamProducer : IProducer
{
    private readonly Dictionary<string, IProducer> _producers = new();
    private readonly SuperStreamProducerConfig _config;
    private readonly DefaultRoutingConfiguration _defaultRoutingConfiguration = new();

    private ProducerConfig FromStreamConfig(string stream)
    {
        return new ProducerConfig()
        {
            Stream = stream,
            ConfirmHandler = _config.ConfirmHandler,
            Reference = _config.Reference,
            MaxInFlight = _config.MaxInFlight,
            ConnectionClosedHandler = _config.ConnectionClosedHandler,
            MetadataHandler = _config.MetadataHandler,
            ClientProvidedName = _config.ClientProvidedName,
            BatchSize = _config.BatchSize,
            MessagesBufferSize = _config.MessagesBufferSize,
        };
    }

    private async Task<IProducer> InitProducer(string stream)
    {
        return await Producer.Create(_config.Streams[stream].ClientParameters,
            FromStreamConfig(stream), _config.Streams[stream].MetaStreamInfo);
    }

    private async Task<IProducer> GetProducer(string stream)
    {
        if (!_producers.ContainsKey(stream))
        {
            _producers.Add(stream, await InitProducer(stream));
        }

        return _producers[stream];
    }

    private SuperStreamProducer(SuperStreamProducerConfig config)
    {
        _config = config;
        _defaultRoutingConfiguration.RoutingStrategy = new HashRoutingMurmurStrategy(_config.RoutingKeyExtractor);
    }

    public ValueTask Send(ulong publishingId, Message message)
    {
        var s = _defaultRoutingConfiguration.RoutingStrategy.Route(message, _config.Streams.Keys.ToList());
        return GetProducer(s[0]).Result.Send(publishingId, message);
    }

    public ValueTask BatchSend(List<(ulong, Message)> messages)
    {
        throw new System.NotImplementedException();
    }

    public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType)
    {
        throw new System.NotImplementedException();
    }

    public Task<ResponseCode> Close()
    {
        throw new System.NotImplementedException();
    }

    public Task<ulong> GetLastPublishingId()
    {
        throw new System.NotImplementedException();
    }

    public bool IsOpen()
    {
        throw new System.NotImplementedException();
    }

    public void Dispose()
    {
        throw new System.NotImplementedException();
    }

    public int MessagesSent { get; }
    public int ConfirmFrames { get; }
    public int IncomingFrames { get; }
    public int PublishCommandsSent { get; }
    public int PendingCount { get; }

    public static IProducer Create(SuperStreamProducerConfig superStreamProducerConfig)
    {
        return new SuperStreamProducer(superStreamProducerConfig);
    }
}

internal record StreamConfiguration
{
    public ClientParameters ClientParameters { get; init; }
    public StreamInfo MetaStreamInfo { get; init; }
}

public record SuperStreamProducerConfig : IProducerConfig
{
    // SuperStreamName. The user interacts with this it
    // In AMQP this is the exchange name
    public string SuperStream { get; set; }

    // streams are the streams created by the super stream
    // The partitioned streams.
    // For example:
    // invoices(super_stream) -> invoices-0, invoices-1, invoices-2
    internal Dictionary<string, StreamConfiguration> Streams { get; set; } = new();

    public Func<Message, string> RoutingKeyExtractor { get; set; } = null;
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
    List<string> Route(Message message, List<string> streams);
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
    public List<string> Route(Message message, List<string> streams)
    {
        var key = _routingKeyExtractor(message);
        var hash = new Murmur32ManagedX86(Seed).ComputeHash(Encoding.UTF8.GetBytes(key));
        var index = BitConverter.ToUInt32(hash, 0) % (uint)streams.Count;
        Console.WriteLine($"{key} -> {streams[(int)index]}- > hash Num:{BitConverter.ToUInt32(hash, 0)}");

        return new List<string>() { streams[(int)index] };
    }

    public HashRoutingMurmurStrategy(Func<Message, string> routingKeyExtractor)
    {
        _routingKeyExtractor = routingKeyExtractor;
    }
}
