// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public class ConnectionPoolConfig
{
    public int MaxConnections { get; set; } = 0;
    public byte ConsumersPerConnection { get; set; } = 1;
    public byte ProducersPerConnection { get; set; } = 1;
}

public class ConnectionItem
{
    public ConnectionItem(string brokerInfo, byte idsPerConnection, IClient client)
    {
        BrokerInfo = brokerInfo;
        ActiveIds = 1;
        LastUsed = DateTime.UtcNow;
        IdsPerConnection = idsPerConnection;
        Client = client;
    }

    public IClient Client { get; }
    public string BrokerInfo { get; }
    public int ActiveIds { get; set; }
    public bool Available => ActiveIds < IdsPerConnection;

    public byte IdsPerConnection { get; }
    public DateTime LastUsed { get; set; }
}

public class ConnectionsPool
{
    private readonly int _maxConnections;
    private readonly byte _itemsPerConnection;

    
    internal static byte FindMissingConsecutive(List<byte> ids)
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
    public ConnectionsPool(int maxConnections, byte itemsPerConnection)
    {
        _maxConnections = maxConnections;
        _itemsPerConnection = itemsPerConnection;
    }

    /// <summary>
    ///  Key: is the client id. And GUID
    ///  Value is the connection item
    /// The Connections contains all the connections created by the pool
    /// </summary>
    internal ConcurrentDictionary<string, ConnectionItem> Connections { get; } = new();

    internal async Task<IClient> GetOrCreateClient(string brokerInfo, Func<Task<IClient>> createClient)
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
            // we need to increment the active items for this connection item
            // ActiveItems that is the producerIds or consumerIds for this connection
            connectionItem.ActiveIds += 1;
            return connectionItem.Client;
        }

        if (_maxConnections > 0 && Connections.Count >= _maxConnections)
        {
            throw new Exception($"Max connections {_maxConnections} reached");
        }

        // no connection available for this brokerInfo
        // let's create a new one
        var client = await createClient().ConfigureAwait(false);
        // the connection give us the client id that is a GUID
        Connections.TryAdd(client.ClientId, new ConnectionItem(brokerInfo, _itemsPerConnection, client));
        return client;
    }

    public void Release(string clientId)
    {
        // given a client id we need to decrement the active items for this connection item
        Connections.TryGetValue(clientId, out var connectionItem);

        // it can be null if the connection is closed in unexpected way
        // so the connection does not exist anymore in the pool
        // we can ignore this case
        if (connectionItem != null)
        {
            connectionItem.ActiveIds -= 1;
        }

        // throw new Exception($"Connection {clientId} not found");
    }

    public void Remove(string clientId)
    {
        // remove the connection from the pool
        // it means that the connection is closed
        Connections.TryRemove(clientId, out _);
    }

    public int Count => Connections.Count;
}
