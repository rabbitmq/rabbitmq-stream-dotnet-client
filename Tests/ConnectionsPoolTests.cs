// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ConnectionsPoolTests
    {
        private static Task<IClient> CreateClient(ClientParameters clientParameters, ILogger logger = null)
        {
            var fake = new FakeClient(clientParameters) { ConnectionProperties = new Dictionary<string, string>() { } };
            return Task.FromResult<IClient>(fake);
        }

        private readonly ITestOutputHelper _testOutputHelper;

        public ConnectionsPoolTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        /// <summary>
        /// Validate the pool consistency when we create a new connection
        /// The connection must be reused when we request a new connection with the same brokerInfo
        /// New connection when we request a new connection with a different brokerInfo or when we reach the available ids
        /// </summary>
        [Fact]
        public async void ValidatePoolConsistency()
        {
            var pool = new ConnectionsPool(0, 10);

            var brokerNode1 = new Broker("node0", 5552);
            var brokerNode2 = new Broker("node1", 5552);
            var brokerNode3 = new Broker("node2", 5552);

            // create the first (fake) connection
            var c1 = await pool.GetOrCreateClient(brokerNode1.ToString(),
                async () => await CreateClient(new ClientParameters()));

            Assert.Equal(1, pool.Count);
            // the ids here is only 1
            Assert.Equal(1, pool.Connections[c1.ClientId].ActiveIds);

            // here we request for a new connection given the same brokerInfo
            var c1_1 = await pool.GetOrCreateClient(brokerNode1.ToString(),
                async () => await CreateClient(new ClientParameters()));

            // we should have the same connection
            Assert.Equal(c1.ClientId, c1_1.ClientId);

            // the pool is always 1 since we reuse the same connection
            Assert.Equal(1, pool.Count);
            // the ids here is 2 since we reuse the same connection
            Assert.Equal(2, pool.Connections[c1_1.ClientId].ActiveIds);

            var c2 = await pool.GetOrCreateClient(brokerNode2.ToString(),
                async () => await CreateClient(new ClientParameters()));

            Assert.Equal(2, pool.Count);
            Assert.Equal(1, pool.Connections[c2.ClientId].ActiveIds);

            var c3 = await pool.GetOrCreateClient(brokerNode3.ToString(),
                async () => await CreateClient(new ClientParameters()));
            Assert.Equal(1, pool.Connections[c3.ClientId].ActiveIds);

            Assert.Equal(3, pool.Count);

            pool.Release(c1.ClientId);
            Assert.Equal(1, pool.Connections[c1.ClientId].ActiveIds);

            pool.Release(c1_1.ClientId);
            Assert.Equal(0, pool.Connections[c1_1.ClientId].ActiveIds);

            pool.Release(c2.ClientId);
            Assert.Equal(0, pool.Connections[c2.ClientId].ActiveIds);

            pool.Release(c3.ClientId);
            Assert.Equal(0, pool.Connections[c3.ClientId].ActiveIds);

            // we release ids so the connection can be used for other ids
            // the pool count is still 3 since we didn't remove the connections
            Assert.Equal(3, pool.Count);

            pool.Remove(c1.ClientId);
            Assert.Equal(2, pool.Count);

            pool.Remove(c2.ClientId);
            Assert.Equal(1, pool.Count);

            pool.Remove(c3.ClientId);
            // removed all the connections from the pool ( due of closing the client)
            Assert.Equal(0, pool.Count);
        }

        // [Fact]
        // public async void RoutingShouldReturnConsistentConnection()
        // {
        // }
    }
}
