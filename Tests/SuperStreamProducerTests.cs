// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

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
        SystemUtils.ResetSuperStreams();
        // RoutingKeyExtractor must be set else the traffic won't be routed
        var system = await StreamSystem.Create(new StreamSystemConfig());
        await Assert.ThrowsAsync<CreateProducerException>(() =>
            system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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
        SystemUtils.ResetSuperStreams();
        // Simple send message to super stream
        // We should not have any errors and according to the routing strategy
        // the message should be routed to the correct stream
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
                Routing = message1 => message1.Properties.MessageId.ToString(),
                Reference = "reference"
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

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4);
        Assert.Equal(await streamProducer.GetLastPublishingId(), (ulong)10);

        Assert.True(streamProducer.MessagesSent == 20);
        SystemUtils.WaitUntil(() => streamProducer.ConfirmFrames > 0);
        SystemUtils.WaitUntil(() => streamProducer.PublishCommandsSent > 0);

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendBachToSuperStream()
    {
        SystemUtils.ResetSuperStreams();
        // Here we are sending a batch of messages to the super stream
        // The number of the messages per queue _must_ be the same as SendMessageToSuperStream test
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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

        await streamProducer.BatchSend(messages);

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // We _must_ have the same number of messages per queue as in the SendMessageToSuperStream test
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4);
        SystemUtils.Wait();

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendSubEntryToSuperStream()
    {
        SystemUtils.ResetSuperStreams();
        // Here we are sending a subentry messages to the super stream
        // The number of the messages per queue _must_ be the same as SendMessageToSuperStream test
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // We _must_ have the same number of messages per queue as in the SendMessageToSuperStream test
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4);
        Assert.Equal(await streamProducer.GetLastPublishingId(), (ulong)1);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendMessageToSuperStreamRecreateConnectionsIfKilled()
    {
        SystemUtils.ResetSuperStreams();
        // This test validates that the super stream producer is able to recreate the connection
        // if the connection is killed
        // It is NOT meant to test the availability of the super stream producer
        // just the reconnect mechanism
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientName = Guid.NewGuid().ToString();
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void ShouldRaiseAObjectDisposedExceptionWhenClose()
    {
        SystemUtils.ResetSuperStreams();

        // This test is for OpenClose Status 
        // When the producer is closed it should raise ObjectDisposedException
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
                Routing = message1 => message1.Properties.MessageId.ToString()
            });
        Assert.True(streamProducer.IsOpen());
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        Assert.False(streamProducer.IsOpen());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await streamProducer.Send(1, new Message(Encoding.Default.GetBytes("hello")));
        });
    }

    [Fact]
    public async void ShouldRaiseAObjectDisposedExceptionWhenCloseWhitDispose()
    {
        SystemUtils.ResetSuperStreams();

        // This test is for using and Dispose  
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
                Routing = message1 => message1.Properties.MessageId.ToString()
            });
        Assert.True(streamProducer.IsOpen());
        streamProducer.Dispose();
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        Assert.False(streamProducer.IsOpen());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await streamProducer.Send(1, new Message(Encoding.Default.GetBytes("hello")));
        });
    }

    [Fact]
    public async void HandleConfirmationToSuperStream()
    {
        SystemUtils.ResetSuperStreams();
        // This test is for the confirmation mechanism
        // We send 20 messages and we should have confirmation messages == stream messages count
        // total count must be 20 divided by 3 streams (not in equals way..)
        var testPassed = new TaskCompletionSource<bool>();
        var confirmedList = new ConcurrentBag<(string, Confirmation)>();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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

        SystemUtils.Wait();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal(9, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream0));
        Assert.Equal(7, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream1));
        Assert.Equal(4, confirmedList.Count(x => x.Item1 == "invoices-2"));

        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void HandleMetaUpdateRemoveSteamShouldContinueToWork()
    {
        SystemUtils.ResetSuperStreams();
        // In this test we are going to remove a stream from the super stream
        // and we are going to check that the producer is still able to send messages
        var confirmed = 0;
        var testPassed = new TaskCompletionSource<bool>();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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
        for (ulong i = 0; i < 10; i++)
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

            await streamProducer.Send(i, message);
        }

        SystemUtils.Wait();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.True(await streamProducer.Close() == ResponseCode.Ok);
        await system.Close();
    }

    [Fact]
    public async void SendMessagesInDifferentWaysShouldAppendToTheStreams()
    {
        SystemUtils.ResetSuperStreams();
        // In this test we are going to send 20 messages with the same message id
        // without reference so the messages in the stream must be appended
        // so the total count must be 20 * 3 (standard send,batch send, subentry send)
        // se also: SuperStreamDeduplicationDifferentWaysShouldGiveSameResults 
        // same scenario but with deduplication
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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

        await streamProducer.BatchSend(batchSendMessages);
        await streamProducer.Send(1, messagesForSubEntry, CompressionType.Gzip);

        SystemUtils.Wait();
        // Total messages must be 20 * 3
        // according to the routing strategy hello{i} that must be the correct routing
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9 * 3);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 3);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4 * 3);
        await system.Close();
    }

    [Fact]
    public async void SuperStreamDeduplicationDifferentWaysShouldGiveSameResults()
    {
        SystemUtils.ResetSuperStreams();
        // In this test we are going to send 20 messages with the same message id
        // and the same REFERENCE, in this way we enable the deduplication
        // so the result messages in the streams but always the same for the first
        // insert. 

        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer =
            await system.CreateSuperStreamProducer(new SuperStreamProducerConfig()
            {
                SuperStream = SystemUtils.InvoicesExchange,
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
        await streamProducer.BatchSend(batchSendMessages);
        await streamProducer.Send(1, messagesForSubEntry, CompressionType.Gzip);

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        // Deduplication in action
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4);
        await system.Close();
    }

    // super stream reliable producer tests
    [Fact]
    public async void ReliableProducerSuperStreamSendMessagesDifferentWays()
    {
        SystemUtils.ResetSuperStreams();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
        {
            StreamSystem = system,
            Stream = SystemUtils.InvoicesExchange,
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

        await streamProducer.BatchSend(batchSendMessages);
        await streamProducer.Send(batchSendMessages, CompressionType.Gzip);

        SystemUtils.Wait();
        // Total messages must be 20 * 3 (standard send,batch send, subentry send)
        // according to the routing strategy hello{i} that must be the correct routing
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9 * 3);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7 * 3);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount("invoices-2") == 4 * 3);
        await system.Close();
    }

    [Fact]
    public async void ReliableProducerHandleConfirmation()
    {
        SystemUtils.ResetSuperStreams();
        // This test is for the confirmation mechanism
        // We send 20 messages and we should have confirmation messages == stream messages count
        // total count must be 20 divided by 3 streams (not in equals way..)
        var testPassed = new TaskCompletionSource<bool>();
        var confirmedList = new ConcurrentBag<(string, Message)>();
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var streamProducer = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
        {
            StreamSystem = system,
            Stream = SystemUtils.InvoicesExchange,
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
        });

        for (ulong i = 0; i < 20; i++)
        {
            var message = new Message(Encoding.Default.GetBytes("hello"))
            {
                Properties = new Properties() { MessageId = $"hello{i.ToString()}" }
            };
            await streamProducer.Send(message);
        }

        SystemUtils.Wait();
        new Utils<bool>(_testOutputHelper).WaitUntilTaskCompletes(testPassed);
        Assert.Equal(9, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream0));
        Assert.Equal(7, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream1));
        Assert.Equal(4, confirmedList.Count(x => x.Item1 == SystemUtils.InvoicesStream2));
        await system.Close();
    }

    [Fact]
    public async void ReliableProducerSendMessageConnectionsIfKilled()
    {
        SystemUtils.ResetSuperStreams();
        // This test validates that the Reliable super stream producer is able to recreate the connection
        // if the connection is killed
        // It is NOT meant to test the availability of the super stream producer
        // just the reconnect mechanism
        var system = await StreamSystem.Create(new StreamSystemConfig());
        var clientName = Guid.NewGuid().ToString();
        var streamProducer = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
        {
            StreamSystem = system,
            Stream = SystemUtils.InvoicesExchange,
            SuperStreamConfig = new SuperStreamConfig()
            {
                Routing = message1 => message1.Properties.MessageId.ToString()
            },
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
                // we just wait a bit to be sure that the connections 
                // will be re-opened
                SystemUtils.Wait(TimeSpan.FromSeconds(1));
            }

            // Here the connection _must_ be recreated  and the message sent
            await streamProducer.Send(message);
        }

        SystemUtils.Wait();
        // Total messages must be 20
        // according to the routing strategy hello{i} that must be the correct routing
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream0) == 9);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream1) == 7);
        SystemUtils.WaitUntil(() => SystemUtils.HttpGetQMsgCount(SystemUtils.InvoicesStream2) == 4);
        await system.Close();
    }
}
