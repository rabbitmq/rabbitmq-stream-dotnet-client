// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    [Collection("Sequential")]
    public class ConsumerSystemTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public ConsumerSystemTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void CreateConsumer()
        {
            var testPassed = new TaskCompletionSource<Data>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig {Reference = "producer", Stream = stream});
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testPassed.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                });
            var msgData = new Data("apple".AsReadonlySequence());
            var message = new Message(msgData);
            await producer.Send(1, message);
            //wait for sent message to be delivered

            new Utils<Data>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(msgData.Contents.ToArray(), testPassed.Task.Result.Contents.ToArray());
            producer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void CloseProducerTwoTimesShouldBeOk()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) => { await Task.CompletedTask; }
                });

            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ConsumerStoreOffset()
        {
            var testPassed = new TaskCompletionSource<int>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            const int numberOfMessages = 10;
            await SystemUtils.PublishMessages(system, stream, numberOfMessages, testOutputHelper);
            var count = 0;
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer_offset",
                    Stream = stream,
                    OffsetSpec = new OffsetTypeFirst(),
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testOutputHelper.WriteLine($"ConsumerStoreOffset receiving.. {count}");
                        count++;
                        if (count == numberOfMessages)
                        {
                            await consumer.StoreOffset(ctx.Offset);
                            testOutputHelper.WriteLine($"ConsumerStoreOffset done: {count}");
                            testPassed.SetResult(numberOfMessages);
                        }

                        await Task.CompletedTask;
                    }
                });

            new Utils<int>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            // // Here we use the standard client to check the offest
            // // since client.QueryOffset/2 is hidden in the System
            //
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var offset = await client.QueryOffset("consumer_offset", stream);
            // The offset must be numberOfMessages less one
            Assert.Equal(offset.Offset, Convert.ToUInt64(numberOfMessages - 1));
            await consumer.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void NotifyConsumerClose()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var testPassed = new TaskCompletionSource<bool>();
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) => { await Task.CompletedTask; },
                    ConnectionClosedHandler = async s =>
                    {
                        testOutputHelper.WriteLine("NotifyConsumerClose set true");
                        testPassed.SetResult(true);
                        await Task.CompletedTask;
                    }
                });

            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void CreateProducerConsumerAddressResolver()
        {
            var testPassed = new TaskCompletionSource<Data>();
            var stream = Guid.NewGuid().ToString();
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Loopback, 5552));
            var config = new StreamSystemConfig() {AddressResolver = addressResolver,};
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig {Reference = "producer", Stream = stream});
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testPassed.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                });
            var msgData = new Data("apple".AsReadonlySequence());
            var message = new Message(msgData);
            await producer.Send(1, message);
            //wait for sent message to be delivered

            new Utils<Data>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(msgData.Contents.ToArray(), testPassed.Task.Result.Contents.ToArray());
            producer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ProducerAndConsumerCompressShouldHaveTheSameMessages()
        {
            void PumpMessages(ICollection<Message> messages, string prefix)
            {
                for (var i = 0; i < 5; i++)
                {
                    messages.Add(new Message(Encoding.UTF8.GetBytes($"{prefix}_{i}")));
                }
            }

            void AssertMessages(IReadOnlyList<Message> expected, IReadOnlyList<Message> actual)
            {
                for (var i = 0; i < 5; i++)
                {
                    Assert.Equal(expected[i].Data.Contents.ToArray(), actual[i].Data.Contents.ToArray());
                }
            }

            var testPassed = new TaskCompletionSource<List<Message>>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));

            var receivedMessages = new List<Message>();
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        receivedMessages.Add(message);
                        if (receivedMessages.Count == 10)
                        {
                            testPassed.SetResult(receivedMessages);
                        }

                        await Task.CompletedTask;
                    }
                });

            var producer = await system.CreateProducer(
                new ProducerConfig {Reference = "producer", Stream = stream});

            var messagesNone = new List<Message>();
            PumpMessages(messagesNone, "None");
            await producer.Send(1, messagesNone, CompressionType.None);

            var messagesGzip = new List<Message>();
            PumpMessages(messagesGzip, "Gzip");
            await producer.Send(2, messagesGzip, CompressionType.Gzip);

            new Utils<List<Message>>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            AssertMessages(messagesNone, testPassed.Task.Result.FindAll(s =>
                Encoding.Default.GetString(s.Data.Contents.ToArray()).Contains("None_")));

            AssertMessages(messagesGzip, testPassed.Task.Result.FindAll(s =>
                Encoding.Default.GetString(s.Data.Contents.ToArray()).Contains("Gzip_")));

            producer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ConsumerValidationAmqpAttributes()
        {
            var testPassed = new TaskCompletionSource<Message>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig {Reference = "producer", Stream = stream});
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testPassed.SetResult(message);
                        await Task.CompletedTask;
                    }
                });
            var msgData = new Data("apple".AsReadonlySequence());
            var message = new Message(msgData)
            {
                Properties = new Properties()
                {
                    Subject = "subject",
                    To = "to",
                    ContentEncoding = "contentEncoding",
                    ContentType = "contentType",
                    CorrelationId = (ulong)5_000_000_000,
                    CreationTime = DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime(),
                    AbsoluteExpiryTime = DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime(),
                    GroupId = "groupId",
                    GroupSequence = 1,
                    MessageId = (long)4_000_000_000,
                    ReplyTo = "replyTo",
                    UserId = new byte[] {0x0, 0xF},
                    ReplyToGroupId = "replyToGroupId"
                },
                Annotations = new Annotations {["akey1"] = "value1", [1] = 1, [1_000_000] = 1_000_000,},
                ApplicationProperties = new ApplicationProperties()
                {
                    ["apkey1"] = "value1",
                    ["apkey2"] = "", //  returns  0x40(Null)  when string is empty or null
                    ["apkey3"] = null, //  returns  0x40(Null)  when string is empty or null 
                }
            };

            await producer.Send(1, message);
            //wait for sent message to be delivered

            new Utils<Message>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(msgData.Contents.ToArray(), testPassed.Task.Result.Data.Contents.ToArray());
            Assert.Equal("subject", testPassed.Task.Result.Properties.Subject);
            Assert.Equal("to", testPassed.Task.Result.Properties.To);
            Assert.Equal("contentEncoding", testPassed.Task.Result.Properties.ContentEncoding);
            Assert.Equal("contentType", testPassed.Task.Result.Properties.ContentType);
            Assert.Equal((ulong)5_000_000_000, testPassed.Task.Result.Properties.CorrelationId);
            Assert.Equal(DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime(),
                testPassed.Task.Result.Properties.AbsoluteExpiryTime);
            Assert.Equal(DateTime.Parse("2008-11-01T19:35:00.0000000Z").ToUniversalTime(),
                testPassed.Task.Result.Properties.CreationTime);
            Assert.Equal("groupId", testPassed.Task.Result.Properties.GroupId);
            Assert.Equal((uint)1, testPassed.Task.Result.Properties.GroupSequence);
            Assert.Equal((long)4_000_000_000, testPassed.Task.Result.Properties.MessageId);
            Assert.Equal("replyTo", testPassed.Task.Result.Properties.ReplyTo);
            Assert.Equal(new byte[] {0x0, 0xF}, testPassed.Task.Result.Properties.UserId);

            Assert.True(testPassed.Task.Result.Annotations.ContainsKey(1));
            Assert.Equal(1, testPassed.Task.Result.Annotations[1]);
            Assert.Equal("value1", testPassed.Task.Result.Annotations["akey1"]);
            Assert.Equal(1_000_000, testPassed.Task.Result.Annotations[1_000_000]);

            Assert.Equal("value1", testPassed.Task.Result.ApplicationProperties["apkey1"]);
            Assert.Null(testPassed.Task.Result.ApplicationProperties["apkey2"]);
            Assert.Null(testPassed.Task.Result.ApplicationProperties["apkey3"]);

            producer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void Amqp091MessagesConsumer()
        {
            // Amqp091 interoperability 
            // We should be able to parse a message coming from an 
            // amqp 901 client
            var testPassed = new TaskCompletionSource<Message>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testPassed.SetResult(message);
                        await Task.CompletedTask;
                    }
                });

            // post AMQP 0-9-1 message 
            var jsonBody =
                "{\"properties\":{" +
                "\"content_type\": \"json\"," +
                "\"message_id\": \"2\"," +
                "\"headers\":{\"hkey\" : \"hvalue\"} },\"routing_key\":" +
                "\"" + stream + "\"" +
                ",\"payload\":\"HI\",\"payload_encoding\":\"string\"}";
            SystemUtils.HttpPost(jsonBody, "exchanges/%2f/amq.default/publish");
            new Utils<Message>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            Assert.Equal("HI", Encoding.Default.GetString(testPassed.Task.Result.Data.Contents.ToArray()));
            Assert.Equal(stream, testPassed.Task.Result.Annotations["x-routing-key"]);
            Assert.Equal("", testPassed.Task.Result.Annotations["x-exchange"]);
            Assert.Equal("hvalue", testPassed.Task.Result.ApplicationProperties["hkey"]);
            Assert.Equal("json", testPassed.Task.Result.Properties.ContentType);
            Assert.Equal("2", testPassed.Task.Result.Properties.MessageId);

            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ConsumerQueryOffset()
        {
            var testPassed = new TaskCompletionSource<int>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            const int numberOfMessages = 10;
            const int numberOfMessagesToStore = 4;
            await SystemUtils.PublishMessages(system, stream, numberOfMessages, testOutputHelper);
            var count = 0;
            const string reference = "consumer_offset";
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = reference,
                    Stream = stream,
                    OffsetSpec = new OffsetTypeOffset(),
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testOutputHelper.WriteLine($"ConsumerStoreOffset receiving.. {count}");
                        count++;
                        if (count == numberOfMessagesToStore)
                        {
                            // store the the offset after numberOfMessagesToStore messages
                            // so when we query the offset we should (must) have the same
                            // values
                            await consumer.StoreOffset(ctx.Offset);
                            testOutputHelper.WriteLine($"ConsumerStoreOffset done: {count}");
                        }

                        if (count == numberOfMessages)
                        {
                            testPassed.SetResult(numberOfMessages);
                        }

                        await Task.CompletedTask;
                    }
                });

            new Utils<int>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            // it may need some time to store the offset
            SystemUtils.Wait();
            // numberOfMessagesToStore index 0
            Assert.Equal((ulong)(numberOfMessagesToStore - 1),
                await system.QueryOffset(reference, stream));

            // this has to raise OffsetNotFoundException in case the offset 
            // does not exist like in this case.
            await Assert.ThrowsAsync<OffsetNotFoundException>(() =>
                system.QueryOffset("reference_does_not_exist", stream));

            await consumer.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ShouldConsumeFromStoredOffset()
        {
            // validate restart consume offset
            // When a consumer from a stored offset with:
            // await system.QueryOffset(Reference, stream);
            // the user has to receive the messages for that offset.
            // The client receive the chuck this is why we need
            // to filter the value client side
            // see Consumer:MaybeDispatch/1
            // For example given 10 messages in a chuck
            // the stored is 7 we need to skip client side the first
            // 6 messages 
            
            var storedOffset = new TaskCompletionSource<ulong>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            const int NumberOfMessages = 10;
            const int NumberOfMessageToStore = 7; // a random number in the interval.

            await SystemUtils.PublishMessages(system, stream, NumberOfMessages, testOutputHelper);
            const string Reference = "consumer_offset";

            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = Reference,
                    Stream = stream,
                    OffsetSpec = new OffsetTypeOffset(),
                    MessageHandler = async (consumer, ctx, _) =>
                    {
                        if (ctx.Offset == NumberOfMessageToStore)
                        {
                            await consumer.StoreOffset(ctx.Offset);
                            storedOffset.SetResult(ctx.Offset);
                        }

                        await Task.CompletedTask;
                    }
                });

            new Utils<ulong>(testOutputHelper).WaitUntilTaskCompletes(storedOffset);

            await consumer.Close();


            // new consumer that should start from stored offset
            var offset = await system.QueryOffset(Reference, stream);
            // the offset received must be the same from the last stored
            Assert.Equal(offset, storedOffset.Task.Result);
            var messagesConsumed = new TaskCompletionSource<ulong>();
            var consumerWithOffset = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = Reference,
                    Stream = stream,
                    OffsetSpec = new OffsetTypeOffset(offset),
                    MessageHandler = async (_, ctx, _) =>
                    {
                        if (ctx.Offset == 7)
                        {
                            // check if the the offset is actually 7
                            messagesConsumed.SetResult(ctx.Offset);
                        }

                        await Task.CompletedTask;
                    }
                });

            new Utils<ulong>(testOutputHelper).WaitUntilTaskCompletes(messagesConsumed);

            SystemUtils.Wait();
            // just a double check 
            Assert.Equal(storedOffset.Task.Result, messagesConsumed.Task.Result);

            await consumer.Close();
            await consumerWithOffset.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }
    }
}
