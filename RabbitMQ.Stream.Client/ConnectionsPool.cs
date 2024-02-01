// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public class ConnectionPoolConfig
{
    /// <summary>
    /// A single TCP connection can handle multiple consumers.
    /// From 1 to 255 consumers per connection.
    /// The default value is 1. So one connection per consumer.
    /// An high value can be useful to reduce the number of connections
    /// but it is not the best for performance.
    /// </summary>
    public byte ConsumersPerConnection { get; set; } = 1;

    /// <summary>
    /// A single TCP connection can handle multiple producers.
    /// From 1 to 255 consumers per connection.
    /// The default value is 1. So one connection per producer.
    /// An high value can be useful to reduce the number of connections
    /// but it is not the best for performance.
    /// </summary>
    public byte ProducersPerConnection { get; set; } = 1;
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

    public bool Available
    {
        get
        {
            var c = Client.Consumers.Count + Client.Publishers.Count;
            return c < IdsPerConnection;
        }
    }

    public int EntitiesCount => Client.Consumers.Count + Client.Publishers.Count;

    public byte IdsPerConnection { get; }
    public DateTime LastUsed { get; set; }
}

/// <summary>
/// ConnectionsPool is a pool of connections for producers and consumers.
/// Each connection can have multiple producers and consumers.
/// Each connection has only producers or consumers not both/mixed.
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

    internal static byte FindNextValidId(List<byte> ids, byte nextId = 0)
    {
        lock (s_lock)
        {
            // // we start with the recycle when we reach the max value
            // // in this way we can avoid to recycle the same ids in a short time
            ids.Sort();
            var l = ids.Where(b => b >= nextId).ToList();
            l.Sort();
            if (l.Count == 0)
            {
                // not necessary to start from 0 because the ids are recycled
                // nextid is passed as parameter to avoid to start from 0
                // see client:IncrementEntityId/0
                return nextId;
            }

            if (l[^1] != byte.MaxValue)
                return (byte)(l[^1] + 1);

            for (var i = 0; i < ids.Count - 1; i++)
            {
                if (l[i + 1] - l[i] > 1)
                {
                    return (byte)(l[i] + 1);
                }
            }

            return (byte)(l[^1] + 1);
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
    private ConcurrentDictionary<string, ConnectionItem> Connections { get; } = new();

    /// <summary>
    /// GetOrCreateClient returns a client for the given brokerInfo.
    /// The broker info is the string representation of the broker ip and port.
    /// See Metadata.cs Broker.ToString() method, ex: Broker(localhost,5552) is "localhost:5552" 
    /// </summary>
    internal async Task<IClient> GetOrCreateClient(string brokerInfo, Func<Task<IClient>> createClient)
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

                if (connectionItem.Client is not { IsClosed: true })
                    return connectionItem.Client;

                // the connection is closed
                // let's remove it from the pool
                Connections.TryRemove(connectionItem.Client.ClientId, out _);
                // let's create a new one
                connectionItem = new ConnectionItem(brokerInfo, _idsPerConnection,
                    await createClient().ConfigureAwait(false));
                Connections.TryAdd(connectionItem.Client.ClientId, connectionItem);

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
            return client;
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
            Connections.TryRemove(clientId, out var connectionItem);
            if (connectionItem == null)
                return;
            connectionItem.Client.Consumers.Clear();
            connectionItem.Client.Publishers.Clear();
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public async Task UpdateSecrets(string newSecret)
    {
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            foreach (var connectionItem in Connections.Values)
            {
                await connectionItem.Client.UpdateSecret(newSecret).ConfigureAwait(false);
                connectionItem.Client.Parameters.Password = newSecret;
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public void MaybeClose(string clientId, string reason)
    {
        _semaphoreSlim.Wait();
        try
        {
            if (!Connections.TryGetValue(clientId, out var connectionItem))
            {
                return;
            }

            if (connectionItem.EntitiesCount > 0)
            {
                return;
            }

            // close the connection
            connectionItem.Client.Close(reason);

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

    /// <summary>
    /// Removes the consumer entity from the client.
    /// When the metadata update is called we need to remove the consumer entity from the client.
    /// </summary>
    public void RemoveConsumerEntityFromStream(string clientId, byte id, string stream)
    {
        _semaphoreSlim.Wait();
        try
        {
            if (!Connections.TryGetValue(clientId, out var connectionItem))
            {
                return;
            }

            connectionItem.Client.Consumers.Where(x =>
                    x.Key == id && x.Value.Item1 == stream).ToList()
                .ForEach(x => connectionItem.Client.Consumers.Remove(x.Key));
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Removes the producer entity from the client.
    /// When the metadata update is called we need to remove the consumer entity from the client.
    /// </summary>
    public void RemoveProducerEntityFromStream(string clientId, byte id, string stream)
    {
        _semaphoreSlim.Wait();
        try
        {
            if (!Connections.TryGetValue(clientId, out var connectionItem))
            {
                return;
            }

            var l = connectionItem.Client.Publishers.Where(x =>
                x.Key == id && x.Value.Item1 == stream).ToList();

            l.ForEach(x => connectionItem.Client.Consumers.Remove(x.Key));
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public int ConnectionsCount => Connections.Count;
}
