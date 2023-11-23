// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ConnectionsPoolTests
    {
        private static Task<IClient> CreateClient(ClientParameters clientParameters)
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

        private class PoolRouting : IRouting
        {
            public Task<IClient> CreateClient(ClientParameters clientParameters, Broker broker, ILogger logger = null)
            {
                var fake = new FakeClient(clientParameters);
                return Task.FromResult<IClient>(fake);
            }

            public bool ValidateDns { get; set; } = false;
        }

        /// <summary>
        /// The connection pool has 1 ids per connection.
        /// Each request for a new connection should return a new connection
        /// </summary>
        [Fact]
        public async void RoutingShouldReturnTwoConnectionsGivenOneItemPerConnection()
        {
            var clientParameters = new ClientParameters { Endpoint = new DnsEndPoint("localhost", 3939) };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("localhost", 3939),
                new List<Broker>());
            var pool = new ConnectionsPool(0, 1);
            var c1 = await RoutingHelper<PoolRouting>.LookupRandomConnection(clientParameters, metaDataInfo, pool);
            var c2 = await RoutingHelper<PoolRouting>.LookupRandomConnection(clientParameters, metaDataInfo, pool);
            // here we have two different connections
            // and must be different since we have only one id per connection
            Assert.NotSame(c1.ClientId, c2.ClientId);
            Assert.Equal(2, pool.Count);
        }

        /// <summary>
        /// The connection pool has 2 ids per connection.
        /// Since we have two ids per connection we can reuse the same connection
        /// </summary>
        [Fact]
        public async void RoutingShouldReturnOneConnectionsGivenTwoIdPerConnection()
        {
            var clientParameters = new ClientParameters { Endpoint = new DnsEndPoint("localhost", 3939) };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("localhost", 3939),
                new List<Broker>());
            var pool = new ConnectionsPool(0, 2);
            var c1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            // here we have one connection with two ids
            Assert.Equal(c1.ClientId, c2.ClientId);
            // two ids per connection
            Assert.Equal(2, pool.Connections[c1.ClientId].ActiveIds);
        }

        /// <summary>
        /// The connection pool has 2 ids per connection.
        /// but we have two different brokerInfo
        /// so we should have two different connections
        /// </summary>
        [Fact]
        public async void RoutingShouldReturnTwoConnectionsGivenOneIdPerConnectionDifferentMetaInfo()
        {
            var clientParameters = new ClientParameters { Endpoint = new DnsEndPoint("Node1", 3939) };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("Node1", 3939),
                new List<Broker>());
            var pool = new ConnectionsPool(0, 2);
            var c1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            Assert.Equal(1, pool.Count);

            var clientParameters1 = new ClientParameters { Endpoint = new DnsEndPoint("Node2", 3939) };
            var metaDataInfo1 =
                new StreamInfo("stream", ResponseCode.Ok, new Broker("Node2", 3939), new List<Broker>());
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters1, metaDataInfo1, pool);
            Assert.NotSame(c1.ClientId, c2.ClientId);
            // even if we have two ids per connection
            // we have two different connections since we have two different brokerInfo
            // so the pool count is 2
            Assert.Equal(2, pool.Count);
        }

        /// <summary>
        /// The pool has 3 ids per connection.
        /// We request 3 connections with the same brokerInfo
        /// then we request a new connection with the same brokerInfo
        /// so we should have two different connections
        /// one with 3 ids and one with 1 id
        /// </summary>
        [Fact]
        public async void RoutingShouldReturnTwoConnectionsGivenTreeIdsForConnection()
        {
            var pool = new ConnectionsPool(0, 3);
            var clientParameters = new ClientParameters { Endpoint = new DnsEndPoint("Node1", 3939) };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("Node1", 3939),
                new List<Broker>());
            var c1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            for (var i = 0; i < 2; i++)
            {
                var c1_1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo,
                    pool);
                Assert.Equal(c1.ClientId, c1_1.ClientId);
            }

            Assert.Equal(3, pool.Connections[c1.ClientId].ActiveIds);
            Assert.Equal(1, pool.Count);
            // here is a new client id since we reach the max ids per connection
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            Assert.NotSame(c1.ClientId, c2.ClientId);
            Assert.Equal(1, pool.Connections[c2.ClientId].ActiveIds);
            Assert.Equal(2, pool.Count);
        }

        /// <summary>
        /// The pool has 3 ids per connection.
        /// We request 3 connections with the same brokerInfo
        /// then release one id and again we request a new connection with the same brokerInfo
        /// so the pool should have one connection with 3 ids 
        /// </summary>
        [Fact]
        public async void ReleaseFromThePoolShouldNotRemoveTheConnection()
        {
            var pool = new ConnectionsPool(0, 3);
            var clientParameters = new ClientParameters { Endpoint = new DnsEndPoint("Node1", 3939) };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("Node1", 3939),
                new List<Broker>());
            var c1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            for (var i = 0; i < 2; i++)
            {
                var c1_1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo,
                    pool);
                Assert.Equal(c1.ClientId, c1_1.ClientId);
            }

            Assert.Equal(3, pool.Connections[c1.ClientId].ActiveIds);
            Assert.Equal(1, pool.Count);
            pool.Release(c1.ClientId);
            Assert.Equal(2, pool.Connections[c1.ClientId].ActiveIds);

            var reusedClient = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo,
                pool);

            // the client id is the same since we reuse the connection
            Assert.Equal(c1.ClientId, reusedClient.ClientId);
            Assert.Equal(3, pool.Connections[c1.ClientId].ActiveIds);

            // we release the connection    
            pool.Release(c1.ClientId);
            Assert.Equal(2, pool.Connections[c1.ClientId].ActiveIds);

            pool.Release(c1.ClientId);
            Assert.Equal(1, pool.Connections[c1.ClientId].ActiveIds);

            pool.Release(c1.ClientId);
            Assert.Equal(0, pool.Connections[c1.ClientId].ActiveIds);

            Assert.Equal(1, pool.Count);
            pool.Remove(c1.ClientId);
            Assert.Equal(0, pool.Count);
        }

        /// <summary>
        /// Test the max connections per pool
        /// In this case there is only one connection per pool with 2 ids
        /// The id 3 requires a new connection
        /// but the pool is full so we raise an exception
        /// </summary>
        [Fact]
        public async void RaiseExceptionWhenThePoolIsFull()
        {
            var pool = new ConnectionsPool(1, 2);
            var clientParameters = new ClientParameters { Endpoint = new DnsEndPoint("Node1", 3939) };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("Node1", 3939),
                new List<Broker>());
            await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            await Assert.ThrowsAsync<TooManyConnectionsException>(async () =>
                await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool));
        }

        /// Integration tests to validate the pool with actual connections
        /// <summary>
        ///  The pool has 2 ids per connection.
        /// Given two consumers for different streams
        /// The pool should have only one connection with 2 ids
        /// </summary>
        [Fact]
        public async void TwoConsumersShouldShareTheSameConnectionFromThePool()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_consumer";
            const string Stream2 = "pool_test_stream_2_consumer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());

            var pool = new ConnectionsPool(0, 2);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var c1 = await RawConsumer.Create(client.Parameters, new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            var c2 = await RawConsumer.Create(client.Parameters, new RawConsumerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);
            // connection pool is 1 since we reuse the same connection
            Assert.Equal(1, pool.Count);
            Assert.Equal(2, pool.Connections.Values.First().ActiveIds);
            await c1.Close();

            Assert.Equal(1, pool.Count);
            Assert.Equal(1, pool.Connections.Values.First().ActiveIds);

            await c2.Close();
            Assert.Equal(0, pool.Count);

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
        }

        /// <summary>
        ///  The pool has 2 ids per connection.
        /// Given two producers for different streams
        /// The pool should have only one connection with 2 ids
        /// </summary>
        [Fact]
        public async void TwoProducersShouldShareTheSameConnectionFromThePool()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_producer";
            const string Stream2 = "pool_test_stream_2_producer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());

            var pool = new ConnectionsPool(0, 2);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var p1 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            var p2 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);

            Assert.Equal(1, pool.Count);
            Assert.Equal(2, pool.Connections.Values.First().ActiveIds);
            await p1.Close();

            Assert.Equal(1, pool.Count);
            Assert.Equal(1, pool.Connections.Values.First().ActiveIds);

            await p2.Close();
            Assert.Equal(0, pool.Count);

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
        }

        /// <summary>
        /// The pool has 1 ids per connection.
        /// So the producer and consumer should have different connections
        /// </summary>
        [Fact]
        public async void TwoProducerAndConsumerShouldHaveDifferentConnection()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_producer";
            const string Stream2 = "pool_test_stream_2_producer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());

            var pool = new ConnectionsPool(0, 1);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var c1 = await RawConsumer.Create(client.Parameters, new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            var p2 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);
            // one for the producer and one for the consumer
            Assert.Equal(2, pool.Count);
            Assert.Equal(1, pool.Connections.Values.First().ActiveIds);
            await c1.Close();

            Assert.Equal(1, pool.Count);
            Assert.Equal(1, pool.Connections.Values.First().ActiveIds);

            await p2.Close();
            Assert.Equal(0, pool.Count);

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
        }

        /// <summary>
        /// Since the consumers and producers share the same connection
        /// in this test we have two consumers and two producers
        /// to be sure the messages are delivered to the right consumer
        /// </summary>
        [Fact]
        public async void DeliverToTheRightConsumerEvenShareTheConnection()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_deliver";
            const string Stream2 = "pool_test_stream_2_deliver";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());
            var testPassedC1 = new TaskCompletionSource<Data>();

            var poolConsumer = new ConnectionsPool(0, 2);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var c1 = await RawConsumer.Create(client.Parameters,
                new RawConsumerConfig(Stream1)
                {
                    Pool = poolConsumer,
                    MessageHandler = async (consumer, context, message) =>
                    {
                        testPassedC1.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                },
                metaDataInfo.StreamInfos[Stream1]);

            var testPassedC2 = new TaskCompletionSource<Data>();
            await RawConsumer.Create(client.Parameters,
                new RawConsumerConfig(Stream2)
                {
                    Pool = poolConsumer,
                    MessageHandler = async (consumer, context, message) =>
                    {
                        testPassedC2.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                },
                metaDataInfo.StreamInfos[Stream2]);

            var poolProducer = new ConnectionsPool(0, 2);

            var p1 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = poolProducer, },
                metaDataInfo.StreamInfos[Stream1]);

            var p2 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = poolProducer },
                metaDataInfo.StreamInfos[Stream2]);

            var msgDataStream1 = new Data("Stream1".AsReadonlySequence());
            var message = new Message(msgDataStream1);
            await p1.Send(1, message);

            var msgDataStream2 = new Data("Stream2".AsReadonlySequence());
            var message2 = new Message(msgDataStream2);
            await p2.Send(1, message2);

            new Utils<Data>(_testOutputHelper).WaitUntilTaskCompletes(testPassedC1);
            new Utils<Data>(_testOutputHelper).WaitUntilTaskCompletes(testPassedC2);
            Assert.Equal(msgDataStream1.Contents.ToArray(), testPassedC1.Task.Result.Contents.ToArray());
            Assert.Equal(msgDataStream2.Contents.ToArray(), testPassedC2.Task.Result.Contents.ToArray());
            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
        }

        /// <summary>
        /// Raise an exception in case the pool is full
        /// </summary>
        [Fact]
        public async void RaiseErrorInCaseThePoolIsFull()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_full";
            const string Stream2 = "pool_test_stream_2_full";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());

            var pool = new ConnectionsPool(1, 1);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var p1 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);

            await Assert.ThrowsAsync<AggregateException>(async () => await RawConsumer.Create(
                client.Parameters, new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]));

            Assert.Equal(1, pool.Count);
            Assert.Equal(1, pool.Connections.Values.First().ActiveIds);

            await p1.Close();
            Assert.Equal(0, pool.Count);

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
        }

        /// <summary>
        /// The pool has 17 ids per connection.
        /// The pool should be consistent in multi thread
        /// Id we create (17* 2) producers in multi thread
        /// the pool must contain only two connections
        /// Same when we close the producers in multi thread the pool must be empty at the end
        /// </summary>
        [Fact]
        public async void TheProducerPoolShouldBeConsistentInMultiThread()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_multi_thread_producer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            const int IdsPerConnection = 17;
            var pool = new ConnectionsPool(0, IdsPerConnection);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1 });
            var producerList = new ConcurrentDictionary<string, IProducer>();

            var tasksP = new List<Task>();
            for (var i = 0; i < (IdsPerConnection * 2); i++)
            {
                tasksP.Add(Task.Run(async () =>
                {
                    var p = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                        metaDataInfo.StreamInfos[Stream1]);
                    producerList.TryAdd(Guid.NewGuid().ToString(), p);
                }));
            }

            await Task.WhenAll(tasksP);

            Assert.Equal(2, pool.Count);
            Assert.Equal(IdsPerConnection, pool.Connections.Values.First().ActiveIds);
            Assert.Equal(IdsPerConnection, pool.Connections.Values.Skip(1).First().ActiveIds);

            var tasksC = new List<Task>();
            producerList.Values.ToList().ForEach(p => tasksC.Add(Task.Run(async () => { await p.Close(); })));
            await Task.WhenAll(tasksC);

            SystemUtils.WaitUntil(() => pool.Count == 0);
            Assert.Equal(0, pool.Count);
            await client.DeleteStream(Stream1);
        }

        // this test doesn't work since the client parameters metadata handler is not an event
        // for the moment I won't change the code. Introduced a new event is a breaking change

        // [Fact]
        // public async void TheProducerPoolShouldBeConsistentWhenAStreamIsDeleted()
        // {
        //     var client = await Client.Create(new ClientParameters() { });
        //     const string Stream1 = "pool_test_stream_1_multi_thread_producer";
        //     await client.CreateStream(Stream1, new Dictionary<string, string>());
        //     const int IdsPerConnection = 2;
        //     var pool = new ConnectionsPool(0, IdsPerConnection);
        //     var metaDataInfo = await client.QueryMetadata(new[] {Stream1});
        //     var producerList = new ConcurrentDictionary<string, IProducer>();
        //
        //     var tasksP = new List<Task>();
        //     for (var i = 0; i < (IdsPerConnection * 1); i++)
        //     {
        //         tasksP.Add(Task.Run(async () =>
        //         {
        //             var p = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1)
        //                 {
        //                     Pool = pool,
        //                 },
        //                 metaDataInfo.StreamInfos[Stream1]);
        //             producerList.TryAdd(Guid.NewGuid().ToString(), p);
        //         }));
        //     }
        //
        //     await Task.WhenAll(tasksP);
        //
        //     Assert.Equal(2, pool.Count);
        //     Assert.Equal(IdsPerConnection, pool.Connections.Values.First().ActiveIds);
        //     Assert.Equal(IdsPerConnection, pool.Connections.Values.Skip(1).First().ActiveIds);
        //
        //     await client.DeleteStream(Stream1);
        //
        //     SystemUtils.WaitUntil(() => pool.Count == 0);
        //     Assert.Equal(0, pool.Count);
        // }

        /// <summary>
        /// The pool has 13 ids per connection.
        /// The pool should be consistent in multi thread
        /// Id we create (13* 2) consumers in multi thread
        /// the pool must contain only two connections
        /// Same when we close the consumers in multi thread the pool must be empty at the end
        /// </summary>
        [Fact]
        public async void TheConsumerPoolShouldBeConsistentInMultiThread()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_multi_thread_consumer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            const int IdsPerConnection = 13;
            var pool = new ConnectionsPool(0, IdsPerConnection);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1 });
            var producerList = new ConcurrentDictionary<string, IConsumer>();

            var tasksP = new List<Task>();
            for (var i = 0; i < (IdsPerConnection * 2); i++)
            {
                tasksP.Add(Task.Run(async () =>
                {
                    producerList.TryAdd(Guid.NewGuid().ToString(),
                        await RawConsumer.Create(client.Parameters,
                            new RawConsumerConfig(Stream1) { Pool = pool },
                            metaDataInfo.StreamInfos[Stream1]));
                }));
            }

            await Task.WhenAll(tasksP);

            Assert.Equal(2, pool.Count);
            Assert.Equal(IdsPerConnection, pool.Connections.Values.First().ActiveIds);
            Assert.Equal(IdsPerConnection, pool.Connections.Values.Skip(1).First().ActiveIds);

            var tasksC = new List<Task>();
            producerList.Values.ToList().ForEach(c => tasksC.Add(Task.Run(async () => { await c.Close(); })));
            await Task.WhenAll(tasksC);

            SystemUtils.WaitUntil(() => pool.Count == 0);
            Assert.Equal(0, pool.Count);
            await client.DeleteStream(Stream1);
        }

        /// The following tests are related to the FindMissingConsecutive method
        /// We need to find the missing consecutive ids
        /// by protocol we can have multi ids per connection so we need to find the missing ids
        /// in case one id is released from the pool
        /// if we start with 0,1,2,3,4,5,6,7,8,9 at some point we release the id 3
        /// the next id should be 3
        /// the id is a byte so we can have 0-255
        [Fact]
        public void FindMissingConsecutiveShouldReturnZeroGivenEmptyList()
        {
            var ids = new List<byte>();
            var missing = ConnectionsPool.FindMissingConsecutive(ids);
            Assert.Equal(0, missing);
        }

        [Fact]
        public void FindMissingConsecutiveShouldReturnOneGivenOneItem()
        {
            var ids = new List<byte>() { 0 };
            var missing = ConnectionsPool.FindMissingConsecutive(ids);
            Assert.Equal(1, missing);
        }

        [Fact]
        public void FindMissingConsecutiveShouldReturnTreeGivenAList()
        {
            var ids = new List<byte>()
            {
                0,
                1,
                2,
                // 3 is missing
                4,
                // 5 is missing
                6,
                // 7 is missing
                8,
                9
            };
            var missing = ConnectionsPool.FindMissingConsecutive(ids);
            Assert.Equal(3, missing);
            ids.Add(3);
            missing = ConnectionsPool.FindMissingConsecutive(ids);
            Assert.Equal(5, missing);

            ids.Add(5);
            missing = ConnectionsPool.FindMissingConsecutive(ids);
            Assert.Equal(7, missing);

            ids.Add(7);
            missing = ConnectionsPool.FindMissingConsecutive(ids);
            Assert.Equal(10, missing);
        }
    }
}
