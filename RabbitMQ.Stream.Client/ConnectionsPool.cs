// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public class ConnectionsPool
{
    private class BrokerInUse
    {
        public BrokerInUse(string brokerInfo)
        {
            BrokerInfo = brokerInfo;
            ActiveItems = 0;
            LastUsed = DateTime.UtcNow;
        }

        public string BrokerInfo { get; }
        public int ActiveItems { get; set; }
        public bool Available => ActiveItems < 50;

        public DateTime LastUsed { get; set; }
    }

    private readonly int _maxConnections;

    public ConnectionsPool(int maxConnections)
    {
        _maxConnections = maxConnections;
    }

    private readonly ConcurrentBag<(BrokerInUse, Task<IClient> client)> _connections = new();

    public Task<IClient> GetOrCreateClient(string brokerInfo, Func<Task<IClient>> createClient)
    {
        var connections = _connections.ToArray();
        var available = connections.FirstOrDefault(c => c.Item1.BrokerInfo == brokerInfo && (c.Item1.Available));
        if (available.Item1 != null)
        {
            available.Item1.ActiveItems += 1;
            available.Item1.LastUsed = DateTime.UtcNow;
            return available.Item2;
        }

        if (connections.Length >= _maxConnections)
        {
            throw new Exception("Max connections reached");
        }

        var client = createClient();
        _connections.Add((new BrokerInUse(brokerInfo), client));
        return client;
    }

    public void Release(string brokerInfo)
    {
        var connections = _connections.ToArray();
        var available = connections.FirstOrDefault(c => c.Item1.BrokerInfo == brokerInfo);
        if (available.Item1 != null)
        {
            available.Item1.ActiveItems -= 1;
        }
    }

    public void Remove(string brokerInfo)
    {
        var connections = _connections.ToArray();
        var available = connections.FirstOrDefault(c => c.Item1.BrokerInfo == brokerInfo);
        if (available.Item1 != null)
        {
            _connections.TryTake(out var _);
        }
    }

    internal sealed class ConnectionsPoolSingleton
    {
        private static readonly Lazy<ConnectionsPool> s_lazy =
            new(() => new ConnectionsPool(100));

        public static ConnectionsPool Instance => s_lazy.Value;
    }
}
