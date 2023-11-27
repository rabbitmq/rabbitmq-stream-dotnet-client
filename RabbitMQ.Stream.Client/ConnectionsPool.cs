// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public class ConnectionPoolConfig
{
    public int MaxConnections { get; set; } = 0;
    public byte ConsumersPerConnection { get; set; } = 1;
    public byte ProducersPerConnection { get; set; } = 1;
}

public class StreamIds
{
    public StreamIds(string stream)
    {
        Stream = stream;
    }

    internal void Acquire()
    {
        Count++;
    }

    internal void Release()
    {
        Count--;
    }

    public int Count { get; private set; } = 0;

    public string Stream { get; }
}

public class ConnectionItem
{
    public ConnectionItem(string brokerInfo, byte idsPerConnection, IClient client)
    {
        BrokerInfo = brokerInfo;
        LastUsed = DateTime.UtcNow;
        IdsPerConnection = idsPerConnection;
        Client = client;
    }

    public IClient Client { get; }
    public string BrokerInfo { get; }

    public Dictionary<string, StreamIds> StreamIds { get; } = new();

    public bool Available
    {
        get
        {
            var c = StreamIds.Values.Sum(streamIdsValue => streamIdsValue.Count);
            return c < IdsPerConnection;
        }
    }

    public byte IdsPerConnection { get; }
    public DateTime LastUsed { get; set; }
}

/// <summary>
/// ConnectionsPool is a pool of connections for producers and consumers.
/// Each connection can have multiple producers and consumers.
/// Each connection has only producers or consumers but not both/mixed.
/// Each IClient has a client id that is a GUID that is the key of the pool.
/// We receive the broker info from the server, so we need to find if there is already a connection
/// with the same broker info and with free slots for producers or consumers.
/// The pool does not trace the producer/consumer ids but just the number of active items.
/// For example if a producer has the ids 2,3,4,6,8,10 the active items are 5.
/// The Tcp Client is responsible to trace the producer/consumer ids.
/// See Client properties:
///   subscriptionIds 
///   publisherIds  
/// </summary>
public class ConnectionsPool
{
    private static readonly object s_lock = new();

    internal static byte FindMissingConsecutive(List<byte> ids)
    {
        lock (s_lock)
        {
            if (ids.Count == 0)
            {
                return 0;
            }

            ids.Sort();
            for (var i = 0; i < ids.Count - 1; i++)
            {
                if (ids[i + 1] - ids[i] > 1)
                {
                    return (byte)(ids[i] + 1);
                }
            }

            return (byte)(ids[^1] + 1);
        }
    }

    private readonly int _maxConnections;
    private readonly byte _idsPerConnection;
    private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

    /// <summary>
    /// Init the pool with the max connections and the max ids per connection
    /// </summary>
    /// <param name="maxConnections"> The max connections are allowed for session</param>
    /// <param name="idsPerConnection"> The max ids per Connection</param>
    public ConnectionsPool(int maxConnections, byte idsPerConnection)
    {
        _maxConnections = maxConnections;
        _idsPerConnection = idsPerConnection;
    }

    /// <summary>
    ///  Key: is the client id a GUID
    ///  Value is the connection item
    ///  The Connections contains all the connections created by the pool
    /// </summary>
    internal ConcurrentDictionary<string, ConnectionItem> Connections { get; } = new();

    /// <summary>
    /// GetOrCreateClient returns a client for the given brokerInfo.
    /// The broker info is the string representation of the broker ip and port.
    /// See Metadata.cs Broker.ToString() method, ex: Broker(localhost,5552) is "localhost:5552" 
    /// </summary>
    internal async Task<IClient> GetOrCreateClient(string brokerInfo, string stream, Func<Task<IClient>> createClient)
    {
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            // do we have a connection for this brokerInfo and with free slots for producer or consumer?
            // it does not matter which connection is available 
            // the important is to have a connection available for the brokerInfo
            var count = Connections.Values.Count(x => x.BrokerInfo == brokerInfo && x.Available);

            if (count > 0)
            {
                // ok we have a connection available for this brokerInfo
                // let's get the first one
                // TODO: we can improve this by getting the connection with the less active items
                var connectionItem = Connections.Values.First(x => x.BrokerInfo == brokerInfo && x.Available);
                connectionItem.LastUsed = DateTime.UtcNow;
                Acquire(connectionItem.Client.ClientId, stream);
                return connectionItem.Client;
            }

            if (_maxConnections > 0 && Connections.Count >= _maxConnections)
            {
                throw new TooManyConnectionsException($"Max connections {_maxConnections} reached");
            }

            // no connection available for this brokerInfo
            // let's create a new one
            var client = await createClient().ConfigureAwait(false);
            // the connection give us the client id that is a GUID
            Connections.TryAdd(client.ClientId, new ConnectionItem(brokerInfo, _idsPerConnection, client));
            Acquire(client.ClientId, stream);
            return client;
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    private void Acquire(string clientId, string stream)
    {
        Connections.TryGetValue(clientId, out var connectionItem);

        if (connectionItem != null)
        {
            connectionItem.StreamIds.TryGetValue(stream, out var streamIds);
            if (streamIds == null)
            {
                streamIds = new StreamIds(stream);
                connectionItem.StreamIds.Add(stream, streamIds);
            }

            streamIds.Acquire();
        }
    }

    public void Release(string clientId, string stream)
    {
        _semaphoreSlim.Wait();
        try
        {
            // given a client id we need to decrement the active items for this connection item
            Connections.TryGetValue(clientId, out var connectionItem);

            // it can be null if the connection is closed in unexpected way
            // so the connection does not exist anymore in the pool
            // we can ignore this case
            if (connectionItem != null)
            {
                connectionItem.StreamIds.TryGetValue(stream, out var streamIds);
                if (streamIds != null)
                {
                    streamIds.Release();
                }
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public void Remove(string clientId)
    {
        _semaphoreSlim.Wait();
        try
        {
            // remove the connection from the pool
            // it means that the connection is closed
            // we don't care if it is called two times for the same connection
            Connections.TryRemove(clientId, out _);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public int ConnectionsCount => Connections.Count;

    public int ActiveIdsCount => Connections.Values.Sum(x => x.StreamIds.Values.Sum(y => y.Count));

    public int ActiveIdsCountForStream(string stream) => Connections.Values.Sum(x =>
        x.StreamIds.TryGetValue(stream, out var streamIds) ? streamIds.Count : 0);

    public int ActiveIdsCountForClient(string clientId) => Connections.TryGetValue(clientId, out var connectionItem)
        ? connectionItem.StreamIds.Values.Sum(y => y.Count)
        : 0;

    public int ActiveIdsCountForClientAndStream(string clientId, string stream) =>
        Connections.TryGetValue(clientId, out var connectionItem) &&
        connectionItem.StreamIds.TryGetValue(stream, out var streamIds)
            ? streamIds.Count
            : 0;
}
