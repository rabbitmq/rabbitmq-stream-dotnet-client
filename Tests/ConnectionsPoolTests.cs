// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
        private static IEnumerable<byte> ConsumersIdsPerConnection(IConsumer consumer)
        {
            var client1 = ((RawConsumer)consumer)._client;
            return client1.consumers.Keys.ToList();
        }

        private static IEnumerable<byte> ProducersIdsPerConnection(IProducer producer)
        {
            var client1 = ((RawProducer)producer)._client;
            return client1.publishers.Keys.ToList();
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
        public void ValidatePoolConsistencyWithMultiBrokers()
        {
            var pool = new ConnectionsPool(0, 10);

            // var brokerNode1 = new Broker("node0", 5552);
            // var brokerNode2 = new Broker("node1", 5552);
            // var brokerNode3 = new Broker("node2", 5552);

            // const string FakeStream = "fake_stream";

            Assert.Equal(0, pool.ConnectionsCount);
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
            var c1 = await RoutingHelper<PoolRouting>.LookupLeaderOrRandomReplicasConnection(clientParameters, metaDataInfo, pool);
            c1.Consumers.Add(1, default);
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderOrRandomReplicasConnection(clientParameters, metaDataInfo, pool);
            c2.Consumers.Add(1, default);
            // here we have two different connections
            // and must be different since we have only one id per connection
            Assert.NotSame(c1.ClientId, c2.ClientId);
            Assert.Equal(2, pool.ConnectionsCount);
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
            // Assert.Equal(2, pool.ActiveIdsCountForClientAndStream(c1.ClientId, metaDataInfo.Stream));
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
            Assert.Equal(1, pool.ConnectionsCount);

            var clientParameters1 = new ClientParameters { Endpoint = new DnsEndPoint("Node2", 3939) };
            var metaDataInfo1 =
                new StreamInfo("stream", ResponseCode.Ok, new Broker("Node2", 3939), new List<Broker>());
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters1, metaDataInfo1, pool);
            Assert.NotSame(c1.ClientId, c2.ClientId);
            // even if we have two ids per connection
            // we have two different connections since we have two different brokerInfo
            // so the pool count is 2
            Assert.Equal(2, pool.ConnectionsCount);
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
            c1.Publishers.Add(0, default);
            for (byte i = 0; i < 2; i++)
            {
                var c1_1 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo,
                    pool);
                c1_1.Publishers.Add((byte)(i + 1), default);
                Assert.Equal(c1.ClientId, c1_1.ClientId);
            }

            Assert.Equal(1, pool.ConnectionsCount);
            // here is a new client id since we reach the max ids per connection
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            Assert.NotSame(c1.ClientId, c2.ClientId);
            // Assert.Equal(1, pool.ActiveIdsCountForClientAndStream(c2.ClientId, metaDataInfo.Stream));
            Assert.Equal(2, pool.ConnectionsCount);
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

            _ = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo,
                pool);

            // the client id is the same since we reuse the connection
            // we release the connection    
            pool.Remove(c1.ClientId);
            Assert.Equal(0, pool.ConnectionsCount);
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
            var c = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            c.Consumers.Add(1, default);
            var c2 = await RoutingHelper<PoolRouting>.LookupLeaderConnection(clientParameters, metaDataInfo, pool);
            c2.Consumers.Add(2, default);
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
            var parameters = new ClientParameters();
            var client = await Client.Create(parameters);
            const string Stream1 = "pool_test_stream_1_consumer";
            const string Stream2 = "pool_test_stream_2_consumer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());

            var pool = new ConnectionsPool(0, 2);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });

            var c1 = await RawConsumer.Create(
                parameters,
                new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            var c2 = await RawConsumer.Create(
                parameters,
                new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);

            Assert.Equal(1, pool.ConnectionsCount);
            await c1.Close();

            Assert.Equal(1, pool.ConnectionsCount);

            await c2.Close();
            Assert.Equal(0, pool.ConnectionsCount);

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
            await client.Close("byte");
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

            Assert.Equal(1, pool.ConnectionsCount);

            Assert.Equal(ResponseCode.Ok, await p1.Close());
            // closing should be idempotent and not affect to the pool
            Assert.Equal(ResponseCode.Ok, await p1.Close());

            Assert.Equal(1, pool.ConnectionsCount);

            Assert.Equal(ResponseCode.Ok, await p2.Close());
            // closing should be idempotent and not affect to the pool
            Assert.Equal(ResponseCode.Ok, await p2.Close());

            Assert.Equal(0, pool.ConnectionsCount);

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
            await client.Close("byte");
        }

        /// <summary>
        /// The pool has 1 ids per connection.
        /// So the producer and consumer should have different connections
        /// </summary>
        [Fact]
        public async void TwoProducerAndConsumerShouldHaveDifferentConnection()
        {
            var parameters = new ClientParameters();
            var client = await Client.Create(parameters);
            const string Stream1 = "pool_test_stream_1_producer";
            const string Stream2 = "pool_test_stream_2_producer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());

            var pool = new ConnectionsPool(0, 1);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var c1 = await RawConsumer.Create(parameters, new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            var p2 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);
            // one for the producer and one for the consumer
            Assert.Equal(2, pool.ConnectionsCount);

            Assert.NotEmpty(ProducersIdsPerConnection(p2).ToList());
            Assert.Equal(0, ProducersIdsPerConnection(p2).ToList()[0]);

            Assert.NotEmpty(ConsumersIdsPerConnection(c1).ToList());
            Assert.Equal(0, ConsumersIdsPerConnection(c1).ToList()[0]);

            Assert.Equal(ResponseCode.Ok, await c1.Close());
            // closing should be idempotent and not affect to the pool
            Assert.Equal(ResponseCode.Ok, await c1.Close());

            Assert.Equal(1, pool.ConnectionsCount);

            // Assert.NotEmpty(ProducersIdsPerConnection(p2).ToList());
            // Assert.Equal(0, ProducersIdsPerConnection(p2).ToList()[0]);

            Assert.Empty(ConsumersIdsPerConnection(c1).ToList());

            await p2.Close();
            Assert.Equal(0, pool.ConnectionsCount);

            Assert.Empty(ProducersIdsPerConnection(p2).ToList());
            Assert.Empty(ConsumersIdsPerConnection(c1).ToList());

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
            await client.Close("byte");
        }

        /// <summary>
        /// Since the consumers and producers share the same connection
        /// in this test we have two consumers and two producers
        /// to be sure the messages are delivered to the right consumer
        /// </summary>
        [Fact]
        public async void DeliverToTheRightConsumerEvenShareTheConnection()
        {
            var parameters = new ClientParameters();
            var client = await Client.Create(parameters);
            const string Stream1 = "pool_test_stream_1_deliver";
            const string Stream2 = "pool_test_stream_2_deliver";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());
            var testPassedC1 = new TaskCompletionSource<Data>();

            var poolConsumer = new ConnectionsPool(0, 2);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var c1 = await RawConsumer.Create(parameters,
                new RawConsumerConfig(Stream1)
                {
                    Pool = poolConsumer,
                    MessageHandler = async (consumer, context, message) =>
                    {
                        testPassedC1.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                }
                , metaDataInfo.StreamInfos[Stream1]
            );

            var testPassedC2 = new TaskCompletionSource<Data>();
            await RawConsumer.Create(parameters,
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
            await client.Close("byte");
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

            Assert.Equal(1, pool.ConnectionsCount);
            Assert.NotEmpty(ProducersIdsPerConnection(p1).ToList());
            Assert.Equal(0, ProducersIdsPerConnection(p1).ToList()[0]);

            await p1.Close();
            Assert.Equal(0, pool.ConnectionsCount);
            Assert.Empty(ProducersIdsPerConnection(p1).ToList());

            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
            await client.Close("byte");
        }

        [Fact]
        public async void ValidateTheRecycleIDs()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_validate_ids";
            await client.CreateStream(Stream1, new Dictionary<string, string>());

            var pool = new ConnectionsPool(0, 50);
            // MetaDataResponse metaDataInfo;
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1 });
            var p = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            for (var i = 0; i < 30; i++)
            {
                var p1 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                    metaDataInfo.StreamInfos[Stream1]);
                Assert.Equal(1, pool.ConnectionsCount);
                Assert.NotEmpty(ProducersIdsPerConnection(p1).ToList());
                var l = ProducersIdsPerConnection(p1).ToList();
                Assert.Equal(i + 1, l[1]);
                await p1.Close();
            }

            await p.Close();
            await client.DeleteStream(Stream1).ConfigureAwait(false);
            await client.Close("byte");
        }

        /// <summary>
        /// In this test we need to check the pool consistency when there is an error during the creation of the producer or consumer
        /// and close the pending connections in case the pool is full.
        /// </summary>
        [Fact]
        public async void PoolShouldBeConsistentWhenErrorDuringCreatingProducerOrConsumer()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "this_stream_does_not_exist";

            var pool = new ConnectionsPool(0, 1);
            var metaDataInfo =
                new StreamInfo(Stream1, ResponseCode.Ok, new Broker("localhost", 5552), new List<Broker>());

            await Assert.ThrowsAsync<CreateConsumerException>(async () => await RawConsumer.Create(
                client.Parameters, new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo));
            Assert.Equal(0, pool.ConnectionsCount);
            Assert.Equal(0, client.consumers.Count);

            await Assert.ThrowsAsync<CreateProducerException>(async () => await RawProducer.Create(
                client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                metaDataInfo));
            Assert.Equal(0, pool.ConnectionsCount);
            Assert.Equal(0, client.consumers.Count);

            const string Stream2 = "consistent_pool_in_case_of_error";
            await client.CreateStream(Stream2, new Dictionary<string, string>());
            var metaDataInfo2 = await client.QueryMetadata(new[] { Stream2 });
            var c1 = await RawConsumer.Create(client.Parameters, new RawConsumerConfig(Stream2) { Pool = pool },
                metaDataInfo2.StreamInfos[Stream2]);

            Assert.Equal(1, pool.ConnectionsCount);
            // try again to fail to see if the pool is still consistent
            await Assert.ThrowsAsync<CreateConsumerException>(async () => await RawConsumer.Create(
                client.Parameters, new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo));

            Assert.Equal(1, pool.ConnectionsCount);
            Assert.Single(ConsumersIdsPerConnection(c1));

            await c1.Close();
            await client.DeleteStream(Stream2).ConfigureAwait(false);
            await client.Close("byte");
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
            for (var i = 0; i < (IdsPerConnection); i++)
            {
                tasksP.Add(Task.Run(async () =>
                {
                    var p = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                        metaDataInfo.StreamInfos[Stream1]);

                    producerList.TryAdd(Guid.NewGuid().ToString(), p);
                }));
            }

            await Task.WhenAll(tasksP);

            producerList.Values.ToList()
                .ForEach(p => Assert.Equal(IdsPerConnection, ProducersIdsPerConnection(p).Count()));

            for (var i = 0; i < (IdsPerConnection); i++)
            {
                tasksP.Add(Task.Run(async () =>
                {
                    var p = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool },
                        metaDataInfo.StreamInfos[Stream1]);
                    producerList.TryAdd(Guid.NewGuid().ToString(), p);
                }));
            }

            await Task.WhenAll(tasksP);
            producerList.Values.ToList()
                .ForEach(p => Assert.Equal(IdsPerConnection, ProducersIdsPerConnection(p).Count()));

            Assert.Equal(2, pool.ConnectionsCount);
            // Assert.Equal(IdsPerConnection * 2, pool.ActiveIdsCountForStream(Stream1));

            var tasksC = new List<Task>();
            producerList.Values.ToList().ForEach(p => tasksC.Add(Task.Run(async () => { await p.Close(); })));
            // called twice should not raise any error due of the _poolSemaphoreSlim in the client
            producerList.Values.ToList().ForEach(p => tasksC.Add(Task.Run(async () => { await p.Close(); })));
            await Task.WhenAll(tasksC);

            SystemUtils.WaitUntil(() => pool.ConnectionsCount == 0);
            Assert.Equal(0, pool.ConnectionsCount);
            // Assert.Equal(0, pool.ActiveIdsCount);
            await client.DeleteStream(Stream1);
            await client.Close("byte");
        }

        /// <summary>
        /// The pool has 3 ids per connection.
        /// Here we test the metadata update event. One connection can handle different
        /// Streams so we need to be sure the pool is consistent when the metadata update handler is raised.
        /// By default the metadata update removes the consumer from the server so we need to remove the consumers
        /// from the pool.
        /// </summary>
        [Fact]
        public async void TheConsumersPoolShouldBeConsistentWhenAStreamIsDeleted()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_delete_consumer";
            const string Stream2 = "pool_test_stream_2_delete_consumer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());
            const int IdsPerConnection = 3;
            var pool = new ConnectionsPool(0, IdsPerConnection);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var iConsumers = new ConcurrentDictionary<string, IConsumer>();

            for (var i = 0; i < (IdsPerConnection * 2); i++)
            {
                var p = await RawConsumer.Create(client.Parameters, new RawConsumerConfig(Stream1) { Pool = pool, },
                    metaDataInfo.StreamInfos[Stream1]);
                iConsumers.TryAdd(Guid.NewGuid().ToString(), p);

                var p2 = await RawConsumer.Create(client.Parameters, new RawConsumerConfig(Stream2) { Pool = pool, },
                    metaDataInfo.StreamInfos[Stream2]);
                iConsumers.TryAdd(Guid.NewGuid().ToString(), p2);
            }

            // Here we have 4 connections ( IdsPerConnection * 2)
            // one per stream
            Assert.Equal(4, pool.ConnectionsCount);
            await client.DeleteStream(Stream1);
            // removed one stream so we should not have active ids for this stream
            // we don't check the connection pool since the connections can be random 
            // so not sure how many connection can we have here. But it doesn't matter since we check the active ids

            await client.DeleteStream(Stream2);
            // here we can check the pool. however the connections  are distributed here must be 0
            SystemUtils.WaitUntil(() => pool.ConnectionsCount == 0);
            // no active ids for the stream2 since we removed the stream

            // no active consumers to the internal consumers list
            iConsumers.Values.ToList().ForEach(
                x => Assert.Empty(ConsumersIdsPerConnection(x)));
        }

        /// <summary>
        /// The pool has 3 ids per connection.
        /// Here we test the metadata update event. One connection can handle different
        /// Streams so we need to be sure the pool is consistent when the metadata update handler is raised.
        /// By default the metadata update removes the producer from the server so we need to remove the producers
        /// from the pool.
        /// </summary>
        [Fact]
        public async void TheProducersPoolShouldBeConsistentWhenAStreamIsDeleted()
        {
            var client = await Client.Create(new ClientParameters() { });
            const string Stream1 = "pool_test_stream_1_delete_producer";
            const string Stream2 = "pool_test_stream_2_delete_producer";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());
            const int IdsPerConnection = 3;
            var pool = new ConnectionsPool(0, IdsPerConnection);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });
            var iProducers = new ConcurrentDictionary<string, IProducer>();

            for (var i = 0; i < (IdsPerConnection * 2); i++)
            {
                var p = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream1) { Pool = pool, },
                    metaDataInfo.StreamInfos[Stream1]);
                iProducers.TryAdd(Guid.NewGuid().ToString(), p);
                var p2 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = pool, },
                    metaDataInfo.StreamInfos[Stream2]);
                iProducers.TryAdd(Guid.NewGuid().ToString(), p2);
            }

            // Here we have 4 connections ( IdsPerConnection * 2)
            // one per stream
            Assert.Equal(4, pool.ConnectionsCount);
            await client.DeleteStream(Stream1);
            // removed one stream so we should not have active ids for this stream
            // we don't check the connection pool since the connections can be random 
            // so not sure how many connection can we have here. But it doesn't matter since we check the active ids

            await client.DeleteStream(Stream2);
            // here we can check the pool. however the connections  are distributed here must be 0
            SystemUtils.WaitUntil(() => pool.ConnectionsCount == 0);
            // no active ids for the stream2 since we removed the stream

            // no active consumers to the internal producers list
            iProducers.Values.ToList().ForEach(
                x => Assert.Empty(ProducersIdsPerConnection(x)));
        }

        /// <summary>
        /// Validate the consistency of the client lists consumers and publishers with
        /// the pool elements.
        /// </summary>
        [Fact]
        public async void ThePoolShouldBeConsistentWhenTheConnectionIsClosed()
        {
            var clientProvidedName = Guid.NewGuid().ToString();
            var client = await Client.Create(new ClientParameters() { ClientProvidedName = clientProvidedName });
            const string Stream1 = "pool_test_stream_1_test_connection_closed";
            const string Stream2 = "pool_test_stream_2_test_connection_closed";
            await client.CreateStream(Stream1, new Dictionary<string, string>());
            await client.CreateStream(Stream2, new Dictionary<string, string>());
            const int IdsPerConnection = 3;
            var pool = new ConnectionsPool(0, IdsPerConnection);
            var metaDataInfo = await client.QueryMetadata(new[] { Stream1, Stream2 });

            var c1 = await RawConsumer.Create(client.Parameters,
                new RawConsumerConfig(Stream1) { Pool = pool },
                metaDataInfo.StreamInfos[Stream1]);

            var c2 = await RawConsumer.Create(client.Parameters,
                new RawConsumerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);

            var p1 = await RawProducer.Create(client.Parameters, new RawProducerConfig(Stream2) { Pool = pool },
                metaDataInfo.StreamInfos[Stream2]);

            Assert.Equal(1, pool.ConnectionsCount);
            SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientProvidedName).Result == 2);
            SystemUtils.WaitUntil(() => pool.ConnectionsCount == 0);
            Assert.Equal(0, pool.ConnectionsCount);
            SystemUtils.WaitUntil(() => ConsumersIdsPerConnection(c1).ToList().Count == 0);
            SystemUtils.WaitUntil(() => ConsumersIdsPerConnection(c2).ToList().Count == 0);
            SystemUtils.WaitUntil(() => ProducersIdsPerConnection(p1).ToList().Count == 0);

            client = await Client.Create(new ClientParameters());
            await client.DeleteStream(Stream1);
            await client.DeleteStream(Stream2);
            await client.Close("bye");
        }

        [Fact]
        public async void ValidatePool()
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await StreamSystem.Create(new StreamSystemConfig() { ConnectionPoolConfig = null }));
        }

        /// The following tests are related to the FindMissingConsecutive method
        /// We need to find the missing consecutive ids
        /// by protocol we can have multi ids per connection so we need to find the missing ids
        /// in case one id is released from the pool
        /// if we start with 0,1,2,3,4,5,6,7,8,9 at some point we release the id 3
        /// the nextid it will be still 10 
        /// The FindNextValidId function will start to reuse the missing ids when the max is reached
        ///  In this way we can reduce the time to use the same ids
        [Fact]
        public void FindNextValidIdShouldReturnZeroGivenEmptyList()
        {
            var ids = new List<byte>();
            var missing = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(0, missing);
        }

        [Fact]
        public void FindNextValidIdShouldReturnTheSameIdGivenEmptyList()
        {
            var ids = new List<byte>();
            var missing = ConnectionsPool.FindNextValidId(ids, 5);
            Assert.Equal(5, missing);
        }

        [Fact]
        public void FindNextValidIdShouldReturnTheNextGivenAStartId()
        {
            var ids = new List<byte>();
            var id = ConnectionsPool.FindNextValidId(ids, 255);
            ids.Add(id);
            Assert.Equal(255, id);
            var next = ConnectionsPool.FindNextValidId(ids);
            ids.Add(next);
            Assert.Equal(0, next);
            Assert.Equal(1, ConnectionsPool.FindNextValidId(ids));
        }

        [Fact]
        public void FindNextValidIdShouldReturnOne()
        {
            var ids = new List<byte>() { 0 };
            var missing = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(1, missing);
        }

        // even there are missing ids the next valid id is the next one
        [Fact]
        public void FindNextValidShouldReturnTreeGivenAList()
        {
            var ids = new List<byte>()
            {
                0,
                1,
                2,
                4,
                6,
                8,
                9
            };
            var nextValidId = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(10, nextValidId);
            ids.Add(10);
            nextValidId = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(11, nextValidId);
            ids.Add(11);
        }

        // in this case we start to recycle the ids
        // since the max is reached
        [Fact]
        public void RecycleIdsWhenTheMaxIsReached()
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
            for (byte i = 10; i < byte.MaxValue; i++)
            {
                ids.Add(i);
            }

            var nextValidId = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(255, nextValidId);
            ids.Add(255);

            nextValidId = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(3, nextValidId);
            ids.Add(3);

            nextValidId = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(5, nextValidId);
            ids.Add(5);

            nextValidId = ConnectionsPool.FindNextValidId(ids);
            Assert.Equal(7, nextValidId);
            ids.Add(7);
        }

        // The ids needs to be always consecutive even there are missing ids
        [Fact]
        public void RecycleIdsWhenTheMaxIsReachedAndStartWithAnId()
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
            // even there are missing ids the next valid id is the next one
            var nextValidId = ConnectionsPool.FindNextValidId(ids, 200);
            Assert.Equal(200, nextValidId);

            // even we start from 3 the next valid id is 10, since we start from the end
            nextValidId = ConnectionsPool.FindNextValidId(ids, 3);
            ids.Add(nextValidId);
            Assert.Equal(10, nextValidId);

            nextValidId = ConnectionsPool.FindNextValidId(ids, 255);
            ids.Add(nextValidId);
            Assert.Equal(255, nextValidId);

            nextValidId = ConnectionsPool.FindNextValidId(ids, 0);
            ids.Add(nextValidId);
            Assert.Equal(3, nextValidId);

            nextValidId = ConnectionsPool.FindNextValidId(ids, 3);
            ids.Add(nextValidId);
            Assert.Equal(5, nextValidId);

            nextValidId = ConnectionsPool.FindNextValidId(ids, 5);
            ids.Add(nextValidId);
            Assert.Equal(7, nextValidId);

            nextValidId = ConnectionsPool.FindNextValidId(ids, 0);
            ids.Add(nextValidId);
            Assert.Equal(11, nextValidId);
        }
    }
}
