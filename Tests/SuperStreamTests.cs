// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class SuperStreamTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    private static void ResetSuperStreams()
    {
        SystemUtils.HttpDeleteExchange("invoices");
        SystemUtils.HttpDeleteQueue("invoices-0");
        SystemUtils.HttpDeleteQueue("invoices-1");
        SystemUtils.HttpDeleteQueue("invoices-2");
        SystemUtils.Wait();
        SystemUtils.HttpPost(
            System.Text.Encoding.Default.GetString(
                SystemUtils.GetFileContent("definition_test.json")), "definitions");
    }

    public SuperStreamTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Serializable]
    public class MessageIdToStream
    {
        public string StreamExpected { get; set; }
        public string MessageId { get; set; }
    }

    [Fact]
    public async void ValidateSuperStreamProducer()
    {
        var system = await StreamSystem.Create(new StreamSystemConfig());

        await Assert.ThrowsAsync<CreateProducerException>(() =>
            system.CreateSuperStreamProducer(new SuperStreamProducerConfig() { SuperStream = "does-not-exist" }));

        await Assert.ThrowsAsync<CreateProducerException>(() =>
            system.CreateSuperStreamProducer(new SuperStreamProducerConfig() { SuperStream = "" }));
        await system.Close();
    }

    [Fact]
    public async void ValidateRoutingKeyProducer()
    {
        ResetSuperStreams();
        // RoutingKeyExtractor must be set else the traffic won't be routed
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await Assert.ThrowsAsync<CreateProducerException>(() =>
            system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = "invoices",
                Routing = null
            }));
        await system.Close();
    }

    private class MessageIdToStreamTestCases : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello1" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello2" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello3" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-03", MessageId = "hello4" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello5" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-03", MessageId = "hello6" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello7" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello8" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello9" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-03", MessageId = "hello10" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello88" } };
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    [Theory]
    [ClassData(typeof(MessageIdToStreamTestCases))]
    public void ValidateHashRoutingStrategy(MessageIdToStream @msg)
    {
        // this test validates that the hash routing strategy is working as expected
        var murmurStrategy = new HashRoutingMurmurStrategy(message => message.Properties.MessageId.ToString());
        var messageTest = new Message(Encoding.Default.GetBytes("hello"))
        {
            Properties = new Properties() { MessageId = msg.MessageId }
        };
        var routes =
            murmurStrategy.Route(messageTest, new List<string>() { "invoices-01", "invoices-02", "invoices-03" });

        Assert.Single(routes);
        Assert.Equal(msg.StreamExpected, routes[0]);
    }

    [Fact]
    public async void SendMessageToSuperStream()
    {
        ResetSuperStreams();
        // Simple send message to super stream
        // We should not have any errors and according to the routing strategy
        // the message should be routed to the correct stream
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = "invoices",
                Routing = message1 => message1.Properties.MessageId.ToString()
            });
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            await streamProducer.Send(i, message);
        }

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-0") == 4);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-1") == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 9);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendBachToSuperStream()
    {
        ResetSuperStreams();
        // Here we are sending a batch of messages to the super stream
        // The number of the messages per queue _must_ be the same as SendMessageToSuperStream test
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = "invoices",
                Routing = message1 => message1.Properties.MessageId.ToString()
            });
        var messages = new List<(ulong, Message)>();
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            messages.Add((i, message));
        }

        await streamProducer.BatchSend(messages);

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // We _must_ have the same number of messages per queue as in the SendMessageToSuperStream test
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-0") == 4);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-1") == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 9);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendSubEntryToSuperStream()
    {
        ResetSuperStreams();
        // Here we are sending a subentry messages to the super stream
        // The number of the messages per queue _must_ be the same as SendMessageToSuperStream test
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = "invoices",
                Routing = message1 => message1.Properties.MessageId.ToString()
            });
        var messages = new List<Message>();
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            messages.Add(message);
        }

        await streamProducer.Send(1, messages, CompressionType.Gzip);

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // We _must_ have the same number of messages per queue as in the SendMessageToSuperStream test
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-0") == 4);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-1") == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 9);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendMessageToSuperStreamRecreateConnectionsIfKilled()
    {
        ResetSuperStreams();
        // This test validates that the super stream producer is able to recreate the connection
        // if the connection is killed
        // It is NOT meant to test the availability of the super stream producer
        // just the reconnect mechanism
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientName = Guid.NewGuid().ToString();
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = "invoices",
                Routing = message1 => message1.Properties.MessageId.ToString(),
                ClientProvidedName = clientName
            });
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };

            if (i == 10)
            {
                SystemUtils.WaitUntil(() => SystemUtils.HttpKillConnections(clientName).Result == 3);
                // We just decide to close the connections
            }

            // Here the connection _must_ be recreated  and the message sent
            await streamProducer.Send(i, message);
        }

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-0") == 4);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-1") == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 9);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }
}
