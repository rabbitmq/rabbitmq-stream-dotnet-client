// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class SuperStreamProducerTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public SuperStreamProducerTests(ITestOutputHelper testOutputHelper)
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
    public async Task ValidateSuperStreamProducer()
    {
        var system = await StreamSystem.Create(new StreamSystemConfig());

        await Assert.ThrowsAsync<CreateProducerException>(() =>
            system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig("does-not-exist")));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig("")));
        await system.Close();
    }

    [Fact]
    public async Task ValidateRoutingKeyProducer()
    {
        await SystemUtils.ResetSuperStreams();
        // RoutingKeyExtractor must be set else the traffic won't be routed
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await Assert.ThrowsAsync<CreateProducerException>(() =>
            system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
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
    public async Task ValidateHashRoutingStrategy(MessageIdToStream @msg)
    {
        // this test validates that the hash routing strategy is working as expected
        var murmurStrategy = new HashRoutingMurmurStrategy(message => message.Properties.MessageId.ToString());
        var messageTest = new Message(Encoding.Default.GetBytes("hello"))
        {
            Properties = new Properties() { MessageId = msg.MessageId }
        };
        var routes =
            await murmurStrategy.Route(messageTest, new List<string>() { "invoices-01", "invoices-02", "invoices-03" });

        Assert.Single(routes);
        Assert.Equal(msg.StreamExpected, routes[0]);
    }

    // ValidateKeyRouting is a test that validates that the key routing strategy is working as expected
    // The key routing strategy is used when the routing key is known before hand
    [Fact]
    public async Task ValidateKeyRouting()
    {
        var messageTest = new Message(Encoding.Default.GetBytes("hello"))
        {
            Properties = new Properties() { MessageId = "italy" }
        };
        // this test validates that the key routing strategy is working as expected
        var keyRoutingStrategy = new KeyRoutingStrategy(message => message.Properties.MessageId.ToString(),
            (_, key) =>
            {
                if (key == "italy")
                {
                    return Task.FromResult(new RouteQueryResponse(1, ResponseCode.Ok, new List<string>() { "italy" }));
                }

                var response = new RouteQueryResponse(1, ResponseCode.Ok, new List<string>() { "" });
                return Task.FromResult(response);
            }, "orders");

        var routes =
            await keyRoutingStrategy.Route(messageTest,
                new List<string>() { "italy", "france", "spain" });

        Assert.Equal("italy", routes[0]);
    }

    [Fact]
    public async Task SendMessageToSuperStream()
    {
        await SystemUtils.ResetSuperStreams();
        // Simple send message to super stream
        // We should not have any errors and according to the routing strategy
        // the message should be routed to the correct stream
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                Reference = "reference",
                Identifier = "my_super_producer_908",
            });
        Assert.True(streamProducer.MessagesSent == 0);
        Assert.True(streamProducer.ConfirmFrames == 0);
        Assert.True(streamProducer.PublishCommandsSent == 0);
        Assert.True(streamProducer.PendingCount == 0);
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            await streamProducer.Send(i, message);
        }

        Assert.Equal("my_super_producer_908", streamProducer.Info.Identifier);
        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        Assert.Equal((ulong)10, await streamProducer.GetLastPublishingId());

        Assert.True(streamProducer.MessagesSent == 20);
        await SystemUtils.WaitUntilAsync(() => streamProducer.ConfirmFrames > 0);
        await SystemUtils.WaitUntilAsync(() => streamProducer.PublishCommandsSent > 0);

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    // SendMessageToSuperStreamWithKeyStrategy is a test that validates that the key routing strategy is working as expected

    [Fact]
    public async Task SendMessageToSuperStreamWithKeyStrategy()
    {
        await SystemUtils.ResetSuperStreams();
        // Simple send message to super stream

        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                Reference = "reference",
                RoutingStrategyType = RoutingStrategyType.Key // this is the key routing strategy
            });
        Assert.True(streamProducer.MessagesSent == 0);
        Assert.True(streamProducer.ConfirmFrames == 0);
        Assert.True(streamProducer.PublishCommandsSent == 0);
        Assert.True(streamProducer.PendingCount == 0);
        var messages = new List<Message>();
        for (ulong i = 0; i < 20; i++)
        {
            // We should not have any errors and according to the routing strategy
            // based on the key the message should be routed to the correct stream
            // the routing keys are:
            // 0,1,2
            var idx = i % 3;
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"{idx}" }
            };
            messages.Add(message);
        }

        ulong id = 0;
        foreach (var message in messages)
        {
            await streamProducer.Send(++id, message);
        }

        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 6);

        Assert.True(streamProducer.MessagesSent == 20);
        await SystemUtils.WaitUntilAsync(() => streamProducer.ConfirmFrames > 0);
        await SystemUtils.WaitUntilAsync(() => streamProducer.PublishCommandsSent > 0);

        var messageNotRouted = new Message(Encoding.Default.GetBytes("hello"))
        {
            Properties = new Properties() { MessageId = "this_is_key_does_not_exist" }
        };
        await Assert.ThrowsAsync<RouteNotFoundException>(async () => await streamProducer.Send(21, messageNotRouted));

        var messagesWithId = messages.Select(message => (++id, message)).ToList();

        await streamProducer.Send(messagesWithId);

        // Total messages must be 20 * 2
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 7 * 2);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 2);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 6 * 2);

        Assert.True(streamProducer.MessagesSent == 20 * 2);

        await Assert.ThrowsAsync<RouteNotFoundException>(async () => await streamProducer.Send(
            new List<(ulong, Message)>() { (55, messageNotRouted) }));

        await streamProducer.Send(++id, messages, CompressionType.Gzip);

        // Total messages must be 20 * 3
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 7 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 6 * 3);

        await Assert.ThrowsAsync<RouteNotFoundException>(async () =>
            await streamProducer.Send(++id,
                new List<Message>() { messageNotRouted }, CompressionType.Gzip));

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async Task SendMessageWithProducerToSuperStreamWithKeyStrategy()
    {
        await SystemUtils.ResetSuperStreams();
        // Simple send message to super stream

        var system = await StreamSystem.Create(new StreamSystemConfig());
        var producer =
            await Producer.Create(new ProducerConfig(system, SystemUtils.InvoicesExchange)
            {
                SuperStreamConfig = new SuperStreamConfig()
                {
                    Routing = message1 => message1.Properties.MessageId.ToString(),
                    RoutingStrategyType = RoutingStrategyType.Key // this is the key routing strategy
                }
            });
        var messages = new List<Message>();
        for (ulong i = 0; i < 20; i++)
        {
            // We should not have any errors and according to the routing strategy
            // based on the key the message should be routed to the correct stream
            // the routing keys are:
            // 0,1,2
            var idx = i % 3;
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"{idx}" }
            };
            messages.Add(message);
        }

        foreach (var message in messages)
        {
            await producer.Send(message);
        }

        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 6);

        var messageNotRouted = new Message(Encoding.Default.GetBytes("hello"))
        {
            Properties = new Properties() { MessageId = "this_is_key_does_not_exist" }
        };
        await Assert.ThrowsAsync<RouteNotFoundException>(async () => await producer.Send(messageNotRouted));

        var messagesWithId = messages.Select(message => (message)).ToList();

        await producer.Send(messagesWithId);

        // Total messages must be 20 * 2
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 7 * 2);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 2);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 6 * 2);

        await Assert.ThrowsAsync<RouteNotFoundException>(async () => await producer.Send(
            new List<Message>() { messageNotRouted }));

        await producer.Send(messages, CompressionType.Gzip);

        // Total messages must be 20 * 3
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 7 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 6 * 3);

        await Assert.ThrowsAsync<RouteNotFoundException>(async () =>
            await producer.Send(
                new List<Message>() { messageNotRouted }, CompressionType.Gzip));
        await producer.Close();
        await system.Close();
    }

    [Fact]
    public async Task SendMessageWithProducerToSuperStreamWithKeyStrategyCountries()
    {
        const string SuperStream = "countries";
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var conf = new BindingsSuperStreamSpec(SuperStream, new[] { "italy", "france", "spain", "germany", "uk" });
        await system.CreateSuperStream(conf);

        var producer =
            await Producer.Create(new ProducerConfig(system, SuperStream)
            {
                SuperStreamConfig = new SuperStreamConfig()
                {
                    Routing = message => message.Properties.MessageId.ToString(),
                    RoutingStrategyType = RoutingStrategyType.Key // this is the key routing strategy
                }
            });
        var messages = new List<Message>()
        {
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "italy"}},
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "italy"}},
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "france"}},
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "spain"}},
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "germany"}},
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "germany"}},
            new(Encoding.Default.GetBytes("hello")) {Properties = new Properties() {MessageId = "uk"}},
        };

        foreach (var message in messages)
        {
            await producer.Send(message);
        }

        Assert.Equal(conf.BindingKeys, new[] { "italy", "france", "spain", "germany", "uk" });
        await SystemUtils.WaitAsync();
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount($"{SuperStream}-italy") == 2);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount($"{SuperStream}-france") == 1);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount($"{SuperStream}-spain") == 1);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount($"{SuperStream}-uk") == 1);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount($"{SuperStream}-germany") == 2);
        await system.DeleteSuperStream(SuperStream);

        await producer.Close();
        await system.Close();
    }

    [Fact]
    public async Task SendBachToSuperStream()
    {
        await SystemUtils.ResetSuperStreams();
        // Here we are sending a batch of messages to the super stream
        // The number of the messages per queue _must_ be the same as SendMessageToSuperStream test
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                Reference = "reference"
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

        await streamProducer.Send(messages);

        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // We _must_ have the same number of messages per queue as in the SendMessageToSuperStream test
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        await SystemUtils.WaitAsync();

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async Task SendSubEntryToSuperStream()
    {
        await SystemUtils.ResetSuperStreams();
        // Here we are sending a subentry messages to the super stream
        // The number of the messages per queue _must_ be the same as SendMessageToSuperStream test
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                Reference = "ref1"
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

        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // We _must_ have the same number of messages per queue as in the SendMessageToSuperStream test
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        Assert.Equal((ulong)1, await streamProducer.GetLastPublishingId());
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async Task SendMessageToSuperStreamRecreateConnectionsIfKilled()
    {
        await SystemUtils.ResetSuperStreams();
        // This test validates that the super stream producer is able to recreate the connection
        // if the connection is killed
        // we use the connection closed handler to recreate the connection
        // the method ReconnectPartition is used to reconnect the partition
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientName = Guid.NewGuid().ToString();

        var c = new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
        {
            Routing = message1 => message1.Properties.MessageId.ToString(),
            ClientProvidedName = clientName,
        };
        var completed = new TaskCompletionSource<bool>();
        var streamProducer = await system.CreateRawSuperStreamProducer(c);
        c.ConnectionClosedHandler = async (reason, stream) =>
        {
            if (reason == ConnectionClosedReason.Normal)
            {
                return;
            }

            var streamInfo = await system.StreamInfo(stream);
            await streamProducer.ReconnectPartition(streamInfo);
            await SystemUtils.WaitAsync();
            completed.SetResult(true);
        };

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };

            if (i == 10)
            {
                await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections($"{clientName}_0").Result == 1);
                await completed.Task;
            }

            // Here the connection _must_ be recreated  and the send the message 
            await streamProducer.Send(i, message);
        }

        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async Task HandleConfirmationToSuperStream()
    {
        await SystemUtils.ResetSuperStreams();
        // This test is for the confirmation mechanism
        // We send 20 messages and we should have confirmation messages == stream messages count
        // total count must be 20 divided by 3 streams (not in equals way..)
        var testPassed = new TaskCompletionSource<bool>();
        var confirmedList = new ConcurrentBag<(string, Confirmation)>();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                ConfirmHandler = conf =>
                {
                    if (conf.Item2.Code == ResponseCode.Ok)
                    {
                        confirmedList.Add((conf.Item1, conf.Item2));
                    }

                    if (confirmedList.Count == 20)
                    {
                        testPassed.SetResult(true);
                    }
                }
            });
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i.ToString()}" }
            };
            await streamProducer.Send(i, message);
        }

        await SystemUtils.WaitAsync();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal(9, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream0));
        Assert.Equal(7, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream1));
        Assert.Equal(4, confirmedList.Count(x => x.Item1 == "invoices-2"));

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async Task HandleMetaUpdateRemoveSteamShouldContinueToWork()
    {
        await SystemUtils.ResetSuperStreams();
        // In this test we are going to remove a stream from the super stream
        // and we are going to check that the producer is still able to send messages
        var confirmed = 0;
        var testPassed = new TaskCompletionSource<bool>();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                ConfirmHandler = conf =>
                {
                    if (conf.Item2.Code == ResponseCode.Ok)
                    {
                        Interlocked.Increment(ref confirmed);
                    }

                    // even we send 10 messages we can stop the test when we have more that 5 confirmed
                    // this because we are going to remove a stream from the super stream
                    // after 5 messages, so have more than 5 is enough
                    if (confirmed > 5)
                    {
                        testPassed.SetResult(true);
                    }
                }
            });
        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };

            if (i == 5)
            {
                // We just decide to remove the stream
                // The metadata update should be propagated to the producer
                // and remove the producer from the producer list
                SystemUtils.HttpDeleteQueue(SystemUtils.InvoicesStream0);
            }

            Thread.Sleep(200);
            try
            {
                await streamProducer.Send(i, message);
            }
            catch (Exception e)
            {
                Assert.True(e is AlreadyClosedException);
            }
        }

        await SystemUtils.WaitAsync();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        // even we removed a stream the producer should be able to send messages and maintain the hash routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async Task SendMessagesInDifferentWaysShouldAppendToTheStreams()
    {
        await SystemUtils.ResetSuperStreams();
        // In this test we are going to send 20 messages with the same message id
        // without reference so the messages in the stream must be appended
        // so the total count must be 20 * 3 (standard send,batch send, subentry send)
        // se also: SuperStreamDeduplicationDifferentWaysShouldGiveSameResults 
        // same scenario but with deduplication
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
            });
        // List for the batch send 
        var batchSendMessages = new List<(ulong, Message)>();
        // List for sub entry messages 
        var messagesForSubEntry = new List<Message>();

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            // we just prepare the lists
            batchSendMessages.Add((i, message));
            messagesForSubEntry.Add(message);

            await streamProducer.Send(i, message);
        }

        await streamProducer.Send(batchSendMessages);
        await streamProducer.Send(1, messagesForSubEntry, CompressionType.Gzip);

        await SystemUtils.WaitAsync();
        // Total messages must be 20 * 3
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4 * 3);
        await streamProducer.Close();
        await system.Close();
    }

    [Fact]
    public async Task SuperStreamDeduplicationDifferentWaysShouldGiveSameResults()
    {
        await SystemUtils.ResetSuperStreams();
        // In this test we are going to send 20 messages with the same message id
        // and the same REFERENCE, in this way we enable the deduplication
        // so the result messages in the streams but always the same for the first
        // insert. 

        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
            {
                Routing = message1 => message1.Properties.MessageId.ToString(),
                Reference = "reference"
            });
        // List for the batch send 
        var batchSendMessages = new List<(ulong, Message)>();
        // List for sub entry messages 
        var messagesForSubEntry = new List<Message>();

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            // we just prepare the lists
            batchSendMessages.Add((i, message));
            messagesForSubEntry.Add(message);

            await streamProducer.Send(i, message);
        }

        // starting form here the number of the messages in the stream must be the same
        // the following send(s) will enable the deduplication
        await streamProducer.Send(batchSendMessages);
        await streamProducer.Send(1, messagesForSubEntry, CompressionType.Gzip);

        await SystemUtils.WaitAsync();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // Deduplication in action
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        await streamProducer.Close();
        await system.Close();
    }

    // super stream reliable producer tests
    [Fact]
    public async Task ReliableProducerSuperStreamSendMessagesDifferentWays()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer = await Producer.Create(new ProducerConfig(system, SystemUtils.InvoicesExchange)
        {
            SuperStreamConfig = new SuperStreamConfig()
            {
                Routing = message1 => message1.Properties.MessageId.ToString()
            }
        }
        );

        var batchSendMessages = new List<Message>();

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            await streamProducer.Send(message);
            batchSendMessages.Add(message);
        }

        await streamProducer.Send(batchSendMessages);
        await streamProducer.Send(batchSendMessages, CompressionType.Gzip);

        await SystemUtils.WaitAsync();
        // Total messages must be 20 * 3 (standard send,batch send, subentry send)
        // according to the routing strategy hello{i} that must be the correct routing
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 3);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4 * 3);
        await streamProducer.Close();
        await system.Close();
    }

    [Fact]
    public async Task ReliableProducerHandleConfirmation()
    {
        await SystemUtils.ResetSuperStreams();
        // This test is for the confirmation mechanism
        // We send 20 messages and we should have confirmation messages == stream messages count
        // total count must be 20 divided by 3 streams (not in equals way..)
        var testPassed = new TaskCompletionSource<bool>();
        var confirmedList = new ConcurrentBag<(string, Message)>();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var config = new ProducerConfig(system, SystemUtils.InvoicesExchange)
        {
            Identifier = "my_super_producer_908",
            SuperStreamConfig =
                new SuperStreamConfig() { Routing = message1 => message1.Properties.MessageId.ToString() },
            ConfirmationHandler = confirmation =>
            {
                if (confirmation.Status == ConfirmationStatus.Confirmed)
                {
                    confirmedList.Add((confirmation.Stream, confirmation.Messages[0]));
                }

                if (confirmedList.Count == 20)
                {
                    testPassed.SetResult(true);
                }

                return Task.CompletedTask;
            }
        };
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += status => { statusInfoReceived.Add(status); };

        var streamProducer = await Producer.Create(config);

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i.ToString()}" }
            };
            await streamProducer.Send(message);
        }

        await SystemUtils.WaitAsync();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal(9, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream0));
        Assert.Equal(7, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream1));
        Assert.Equal(4, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream2));
        await streamProducer.Close();
        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[1].To);

        Assert.Equal("my_super_producer_908", statusInfoReceived[0].Identifier);
        Assert.Equal("my_super_producer_908", statusInfoReceived[1].Identifier);

        Assert.Equal(SystemUtils.InvoicesExchange, statusInfoReceived[0].Stream);
        Assert.Equal(SystemUtils.InvoicesExchange, statusInfoReceived[1].Stream);
        await system.Close();
    }

    [Fact]
    public async Task ReliableProducerSendMessageConnectionsIfKilled()
    {
        await SystemUtils.ResetSuperStreams();
        // This test validates that the Reliable super stream producer is able to recreate the connection
        // if the connection is killed
        // It is NOT meant to test the availability of the super stream producer
        // just the reconnect mechanism
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientName = Guid.NewGuid().ToString();
        var testPassed = new TaskCompletionSource<bool>() { };
        var statusCompleted = new TaskCompletionSource<bool>() { };
        var received = 0;
        var error = 0;
        var config = new ProducerConfig(system, SystemUtils.InvoicesExchange)
        {
            SuperStreamConfig =
                new SuperStreamConfig() { Routing = message1 => message1.Properties.MessageId.ToString() },
            TimeoutMessageAfter = TimeSpan.FromSeconds(1),
            ConfirmationHandler = async confirmation =>
            {
                if (confirmation.Status != ConfirmationStatus.Confirmed)
                {
                    Interlocked.Increment(ref error);
                }

                if (Interlocked.Increment(ref received) == 20)
                {
                    testPassed.SetResult(true);
                }

                await Task.CompletedTask;
            },
            ClientProvidedName = clientName,
            ReconnectStrategy = new TestBackOffReconnectStrategy()
        };
        var statusInfoReceived = new List<StatusInfo>();
        config.StatusChanged += status =>
        {
            statusInfoReceived.Add(status);
            if (statusInfoReceived.Count == 3)
            {
                statusCompleted.SetResult(true);
            }
        };

        var streamProducer = await Producer.Create(config);

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i}" }
            };
            if (i == 10)
            {
                // We just decide to close the connections
                // The messages will go in time out since not confirmed
                await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpKillConnections($"{clientName}_0").Result == 1);
            }

            // Here the connection _must_ be recreated  and the message sent
            await streamProducer.Send(message);
        }

        await SystemUtils.WaitAsync();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        // killed the connection for the InvoicesStream0. So received + error must be 9
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) + error == 9);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        await SystemUtils.WaitUntilAsync(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);

        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(statusCompleted);

        Assert.Equal(ReliableEntityStatus.Initialization, statusInfoReceived[0].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[0].To);
        //  statusInfoReceived[0].Partitions
        Assert.Contains(SystemUtils.InvoicesStream0, statusInfoReceived[0].Partitions);
        Assert.Contains(SystemUtils.InvoicesStream1, statusInfoReceived[0].Partitions);
        Assert.Contains(SystemUtils.InvoicesStream2, statusInfoReceived[0].Partitions);

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[1].From);
        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[1].To);
        Assert.Equal(ChangeStatusReason.UnexpectedlyDisconnected, statusInfoReceived[1].Reason);
        Assert.Contains(SystemUtils.InvoicesStream0, statusInfoReceived[1].Partitions);

        Assert.Equal(SystemUtils.InvoicesExchange, statusInfoReceived[1].Stream);

<<<<<<< TODO: Unmerged change from project 'Tests(net9.0)', Before:
        Assert.Equal(SystemUtils.InvoicesStream0, statusInfoReceived[1].Partitions[0]);;
=======
        Assert.Equal(SystemUtils.InvoicesStream0, statusInfoReceived[1].Partitions[0]);
        ;
>>>>>>> After
        Assert.Equal(SystemUtils.InvoicesStream0, statusInfoReceived[1].Partitions[0]);
        ;

        Assert.Equal(ReliableEntityStatus.Reconnection, statusInfoReceived[2].From);
        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[2].To);
        Assert.Equal(ChangeStatusReason.None, statusInfoReceived[2].Reason);

        await streamProducer.Close();

        Assert.Equal(ReliableEntityStatus.Open, statusInfoReceived[3].From);
        Assert.Equal(ReliableEntityStatus.Closed, statusInfoReceived[3].To);
        Assert.Equal(ChangeStatusReason.ClosedByUser, statusInfoReceived[3].Reason);
        Assert.Contains(SystemUtils.InvoicesStream0, statusInfoReceived[3].Partitions);
        Assert.Contains(SystemUtils.InvoicesStream1, statusInfoReceived[3].Partitions);
        Assert.Contains(SystemUtils.InvoicesStream2, statusInfoReceived[3].Partitions);

        await system.Close();
    }

    [Fact]
    public async Task ReliableProducerSuperStreamInfoShouldBeTheSame()
    {
        await SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var producerConfig = new ProducerConfig(system, SystemUtils.InvoicesExchange)
        {
            SuperStreamConfig =
                new SuperStreamConfig() { Routing = message1 => message1.Properties.MessageId.ToString() },
        };
        var producer = await Producer.Create(producerConfig);
        Assert.Equal(SystemUtils.InvoicesExchange, producer.Info.Stream);
        Assert.Contains(SystemUtils.InvoicesStream0, producer.Info.Partitions);
        Assert.Contains(SystemUtils.InvoicesStream1, producer.Info.Partitions);
        Assert.Contains(SystemUtils.InvoicesStream2, producer.Info.Partitions);
        await producer.Close();

    }
}
