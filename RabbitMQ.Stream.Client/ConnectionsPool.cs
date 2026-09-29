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

public enum ConnectionClosePolicy
{
    /// <summary>
    /// The connection is closed when the last consumer or producer is removed.
    /// </summary>
    CloseWhenEmpty,

    /// <summary>
    /// The connection is closed when the last consumer or producer is removed and the connection is not used for a certain time.
    /// </summary>
    CloseWhenEmptyAndIdle
}

public class ConnectionCloseConfig
{
    /// <summary>
    /// Policy to close the connection.
    /// </summary>

    public ConnectionClosePolicy Policy { get; set; } = ConnectionClosePolicy.CloseWhenEmpty;

    /// <summary>
    /// The connection is closed when the last consumer or producer is removed and the connection is not used for a certain time.
    /// Idle time is valid only if the policy is CloseWhenEmptyAndIdle.
    /// </summary>
    public TimeSpan IdleTime { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Interval to check the idle time.
    /// Default is high because the check is done in a separate thread.
    /// The field is internal to help the test.
    /// </summary>
    internal TimeSpan CheckIdleTime { get; set; } = TimeSpan.FromSeconds(60);
}

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
    /// From 1 to 255 producers per connection.
    /// The default value is 1. So one connection per producer.
    /// An high value can be useful to reduce the number of connections
    /// but it is not the best for performance.
    /// </summary>
    public byte ProducersPerConnection { get; set; } = 1;

    /// <summary>
    ///  Define the connection close policy.
    /// </summary>
    public ConnectionCloseConfig ConnectionCloseConfig { get; set; } = new ConnectionCloseConfig();
}

public class LastSecret
{
    public string Secret { get; private set; } = string.Empty;
    public DateTime LastUpdate { get; private set; } = DateTime.MinValue;
    public bool IsValid => LastUpdate > DateTime.MinValue && !string.IsNullOrEmpty(Secret);

    public void Update(string secret)
    {
        Secret = secret;
        LastUpdate = DateTime.UtcNow;
    }
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

    public bool Available => EntitiesCount + Reservations < IdsPerConnection;

    public int EntitiesCount => Client.Consumers.Count + Client.Publishers.Count;

    /// <summary>
    /// Slots handed out by the pool to producers or consumers that are not registered yet.
    /// The pool returns the client before the entity is added to the client (DeclarePublisher/Subscribe),
    /// so without the reservation the same slot could be given twice, or the connection could be closed
    /// because it looks empty.
    /// It is changed only under the pool lock.
    /// </summary>
    internal int Reservations { get; set; }

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
public class ConnectionsPool : IDisposable
{
    private static readonly object s_lock = new();
    private bool _isRunning = false;

    internal static byte FindNextValidId(List<byte> ids, byte nextId = 0)
    {
        lock (s_lock)
        {
            var sortedIds = ids.ToList();
            sortedIds.Sort();
            var idsAtOrAboveNext = sortedIds.Where(b => b >= nextId).ToList();
            if (idsAtOrAboveNext.Count == 0)
            {
                // not necessary to start from 0 because the ids are recycled
                // nextid is passed as parameter to avoid to start from 0
                // see client:IncrementEntityId/0
                return nextId;
            }

            if (idsAtOrAboveNext[^1] != byte.MaxValue)
            {
                return (byte)(idsAtOrAboveNext[^1] + 1);
            }

            // let's try to find a free id in the list
            var idSet = ids.ToHashSet();
            for (byte i = 0; i < byte.MaxValue; i++)
            {
                if (!idSet.Contains(i))
                {
                    return i;
                }
            }

            throw new InvalidOperationException("No more available ids");
        }
    }

    private readonly int _maxConnections;
    private readonly byte _idsPerConnection;
    private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);
    private readonly LastSecret _lastSecret = new();
    private readonly Task _checkIdleConnectionTimeTask;

    /// <summary>
    /// A connection that is being opened outside the lock.
    /// Other requests for the same broker can reserve a slot on it and wait for it,
    /// so a burst of requests (ex: reconnection) does not open more connections than needed.
    /// </summary>
    private class PendingConnection
    {
        public PendingConnection(string brokerInfo)
        {
            BrokerInfo = brokerInfo;
        }

        public string BrokerInfo { get; }
        public int Reservations { get; set; } = 1;

        public TaskCompletionSource<IClient> Client { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    // changed only under the lock. They count for the max connections.
    private readonly List<PendingConnection> _pendingConnections = new();

    /// <summary>
    /// Init the pool with the max connections and the max ids per connection
    /// </summary>
    /// <param name="maxConnections"> The max connections are allowed for session</param>
    /// <param name="idsPerConnection"> The max ids per Connection</param>
    /// <param name="connectionCloseConfig"> Policy to close the connections in the pool</param>
    public ConnectionsPool(int maxConnections, byte idsPerConnection, ConnectionCloseConfig connectionCloseConfig)
    {
        _maxConnections = maxConnections;
        _idsPerConnection = idsPerConnection;
        ConnectionPoolConfig = connectionCloseConfig;
        _isRunning = true;
        _checkIdleConnectionTimeTask = ConnectionPoolConfig.Policy == ConnectionClosePolicy.CloseWhenEmptyAndIdle
            ? Task.Run(CheckIdleConnectionTime)
            : Task.CompletedTask;
    }

    private ConnectionCloseConfig ConnectionPoolConfig { get; }

    private async Task CheckIdleConnectionTime()
    {
        while (_isRunning)
        {
            await Task.Delay(ConnectionPoolConfig.CheckIdleTime)
                .ConfigureAwait(false);

            try
            {
                // the lock is needed to not close a connection reserved by GetOrCreateClient
                await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // the pool is disposed
                return;
            }

            try
            {
                var connectionItems = Connections.Values.ToList();
                var now = DateTime.UtcNow;

                // Shutting down: close all empty connections
                // Running: close only empty connections that have been idle for IdleTime
                foreach (var connectionItem in connectionItems.Where(c =>
                             IsEmpty(c) &&
                             (!_isRunning || c.LastUsed.Add(ConnectionPoolConfig.IdleTime) < now)))
                {
                    CloseItemAndConnection("Idle connection", connectionItem);
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }
    }

    private static bool IsEmpty(ConnectionItem connectionItem) =>
        connectionItem.EntitiesCount == 0 && connectionItem.Reservations == 0;

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
    /// The returned client has a slot reserved for the caller.
    /// The caller must call <see cref="ReleaseReservation"/> once the producer or consumer
    /// is registered on the client, or when the registration failed.
    /// </summary>
    internal async Task<IClient> GetOrCreateClient(string brokerInfo, Func<Task<IClient>> createClient)
    {
        PendingConnection pendingConnection;
        Task<IClient> connectionTask;
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            var connectionItems = Connections.Values.Where(x => x.BrokerInfo == brokerInfo && x.Available)
                .ToLookup(x => x.Client.IsClosed);

            // remove closed connections
            foreach (var closedItem in connectionItems[true])
            {
                Connections.TryRemove(closedItem.Client.ClientId, out _);
            }

            var connectionItem = connectionItems[false].OrderBy(x => x.EntitiesCount + x.Reservations)
                .FirstOrDefault();
            if (connectionItem != null)
            {
                connectionItem.Reservations++;
                connectionItem.LastUsed = DateTime.UtcNow;
                return connectionItem.Client;
            }

            // a connection to the same broker is being opened and it has free slots
            // reserve a slot and wait for it
            var waitingFor = _pendingConnections.FirstOrDefault(x =>
                x.BrokerInfo == brokerInfo && x.Reservations < _idsPerConnection);
            if (waitingFor != null)
            {
                waitingFor.Reservations++;
                pendingConnection = null;
                connectionTask = waitingFor.Client.Task;
            }
            else
            {
                if (_maxConnections > 0 && Connections.Count + _pendingConnections.Count >= _maxConnections)
                {
                    throw new TooManyConnectionsException($"Max connections {_maxConnections} reached");
                }

                pendingConnection = new PendingConnection(brokerInfo);
                _pendingConnections.Add(pendingConnection);
                connectionTask = pendingConnection.Client.Task;
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }

        if (pendingConnection == null)
        {
            // the connection is opened by another request
            return await connectionTask.ConfigureAwait(false);
        }

        // The connection is opened outside the lock.
        // It can take a long time (retries, handshake, unreachable broker) and it must not block
        // the other producers and consumers that use the pool, for example during a reconnection.
        try
        {
            var client = await createClient().ConfigureAwait(false);
            await AddPendingConnection(pendingConnection, client).ConfigureAwait(false);
            pendingConnection.Client.SetResult(client);
        }
        catch (Exception e)
        {
            await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                _pendingConnections.Remove(pendingConnection);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            // the requests waiting for this connection fail as well
            pendingConnection.Client.SetException(e);
        }

        return await connectionTask.ConfigureAwait(false);
    }

    private async Task AddPendingConnection(PendingConnection pendingConnection, IClient client)
    {
        await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
        try
        {
            _pendingConnections.Remove(pendingConnection);
            var connectionItem = new ConnectionItem(pendingConnection.BrokerInfo, _idsPerConnection, client)
            {
                Reservations = pendingConnection.Reservations
            };
            Connections.TryAdd(client.ClientId, connectionItem);

            // the secret was updated while the connection was being opened
            // so the new connection uses the old secret
            if (_lastSecret.IsValid && client.Parameters?.Password != _lastSecret.Secret)
            {
                try
                {
                    await client.UpdateSecret(_lastSecret.Secret).ConfigureAwait(false);
                }
                catch
                {
                    // the requests won't get the client, so they can't release the reservations
                    CloseItemAndConnection("Secret update failed", connectionItem);
                    throw;
                }
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Releases the slot reserved by <see cref="GetOrCreateClient"/>.
    /// To call when the producer or consumer is registered on the client or when the registration failed.
    /// If the connection is empty it is closed following the close policy.
    /// </summary>
    internal void ReleaseReservation(string clientId, string reason)
    {
        _semaphoreSlim.Wait();
        try
        {
            if (!Connections.TryGetValue(clientId, out var connectionItem))
            {
                return;
            }

            if (connectionItem.Reservations > 0)
            {
                connectionItem.Reservations--;
            }

            MaybeCloseItem(connectionItem, reason);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public bool TryMergeClientParameters(ClientParameters clientParameters, out ClientParameters cp)
    {
        if (!_lastSecret.IsValid || clientParameters.Password == _lastSecret.Secret)
        {
            cp = clientParameters;
            return false;
        }

        cp = clientParameters with { Password = _lastSecret.Secret };
        return true;
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
            _lastSecret.Update(newSecret);
            foreach (var connectionItem in Connections.Values)
            {
                await connectionItem.Client.UpdateSecret(newSecret).ConfigureAwait(false);
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

            MaybeCloseItem(connectionItem, reason);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    // to call under the lock
    private void MaybeCloseItem(ConnectionItem connectionItem, string reason)
    {
        // the connection is not closed if a producer or consumer is about to use it
        if (!IsEmpty(connectionItem))
        {
            return;
        }

        connectionItem.LastUsed = DateTime.UtcNow;

        if (ConnectionPoolConfig.Policy == ConnectionClosePolicy.CloseWhenEmpty)
        {
            CloseItemAndConnection(reason, connectionItem);
        }
    }

    private void CloseItemAndConnection(string reason, ConnectionItem connectionItem)
    {
        // close the connection
        connectionItem.Client.Close(reason);
        // remove the connection from the pool
        // it means that the connection is closed
        // we don't care if it is called two times for the same connection
        Connections.TryRemove(connectionItem.Client.ClientId, out _);
    }

    private int PendingConnections => Connections.Values.Count(x => x.EntitiesCount > 0);

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

            var keysToRemove = connectionItem.Client.Consumers
                .Where(x => x.Key == id && x.Value.Item1 == stream)
                .Select(x => x.Key)
                .ToList();
            foreach (var key in keysToRemove)
            {
                connectionItem.Client.Consumers.Remove(key);
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    /// <summary>
    /// Removes the producer entity from the client.
    /// When the metadata update is called we need to remove the producer entity from the client.
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

            var keysToRemove = connectionItem.Client.Publishers
                .Where(x => x.Key == id && x.Value.Item1 == stream)
                .Select(x => x.Key)
                .ToList();
            foreach (var key in keysToRemove)
            {
                connectionItem.Client.Publishers.Remove(key);
            }
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public int ConnectionsCount => Connections.Count;

    public async Task Close()
    {
        // The pool can't be closed if there are pending connections with the policy: CloseWhenEmptyAndIdle 
        // else there is no way to close the pending connections.
        // The user needs to close the pending connections before to close the pool.
        // At the moment when the pool is closed the pending connections are not closed with CloseWhenEmpty
        // because the pool is not strictly bound to the stream system.
        // The StreamSystem doesn't close the connections when it is closed. That was by design
        // We could consider (Version 2.0) to close all the Producers and Consumers and their connection when the StreamSystem is closed.
        // Other clients like Java and Golang close the connections when the Environment (alias StreamSystem) is closed.
        if (PendingConnections > 0 && ConnectionPoolConfig.Policy == ConnectionClosePolicy.CloseWhenEmptyAndIdle)
        {
            throw new PendingConnectionsException(
                $"There are {PendingConnections} pending connections. With the policy CloseWhenEmptyAndIdle you need to close them");
        }

        _isRunning = false;
        if (!_checkIdleConnectionTimeTask.IsCompleted)
        {
            await _checkIdleConnectionTimeTask.ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        _isRunning = false;
        _semaphoreSlim.Dispose();
        GC.SuppressFinalize(this);
    }
}
