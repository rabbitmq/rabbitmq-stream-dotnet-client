// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ConsumerSystemTests
    {
        private readonly ICrc32 _crc32 = new Crc32();
        private readonly ITestOutputHelper testOutputHelper;

        public ConsumerSystemTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void CreateRawConsumer()
        {
            var testPassed = new TaskCompletionSource<Data>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var rawProducer = await system.CreateRawProducer(
                new RawProducerConfig(stream) { Reference = "producer" });
            var identifierReceived = "";
            uint messagesInTheChunk = 0;
            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    Identifier = "consumer_identifier_999",
                    MessageHandler = async (sourceConsumer, ctx, message) =>
                    {
                        messagesInTheChunk = ctx.ChuckMessagesCount;
                        identifierReceived = sourceConsumer.Info.Identifier;
                        testPassed.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                });
            var msgData = new Data("apple".AsReadonlySequence());
            var message = new Message(msgData);
            await rawProducer.Send(1, message);
            //wait for sent message to be delivered

            new Utils<Data>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(msgData.Contents.ToArray(), testPassed.Task.Result.Contents.ToArray());
            Assert.Equal("consumer_identifier_999", identifierReceived);
            Assert.Equal((uint)1, messagesInTheChunk);
            rawProducer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void CloseProducerTwoTimesShouldBeOk()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    MessageHandler = async (consumer, ctx, message) => { await Task.CompletedTask; }
                });

            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ConsumerStoreOffset()
        {
            var testPassed = new TaskCompletionSource<int>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            const int NumberOfMessages = 10;
            await SystemUtils.PublishMessages(system, stream, NumberOfMessages, testOutputHelper);
            var count = 0;
            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer_offset",
                    OffsetSpec = new OffsetTypeFirst(),
                    MessageHandler = async (consumer, ctx, _) =>
                    {
                        testOutputHelper.WriteLine($"ConsumerStoreOffset receiving.. {count}");
                        count++;
                        if (count == NumberOfMessages)
                        {
                            await consumer.StoreOffset(ctx.Offset);
                            testOutputHelper.WriteLine($"ConsumerStoreOffset done: {count}");
                            testPassed.SetResult(NumberOfMessages);
                        }

                        await Task.CompletedTask;
                    }
                });

            new Utils<int>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            SystemUtils.Wait();
            // // Here we use the standard client to check the offest
            // // since client.QueryOffset/2 is hidden in the System
            //
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var offset = await client.QueryOffset("consumer_offset", stream);
            // The offset must be numberOfMessages less one
            Assert.Equal(offset.Offset, Convert.ToUInt64(NumberOfMessages - 1));
            await consumer.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void NotifyConsumerClose()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var testPassed = new TaskCompletionSource<bool>();
            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
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
        public async void CreateProducerConsumerAddressResolver()
        {
            var testPassed = new TaskCompletionSource<Data>();
            var stream = Guid.NewGuid().ToString();
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Loopback, 5552));
            var config = new StreamSystemConfig() { AddressResolver = addressResolver, };
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var rawProducer = await system.CreateRawProducer(
                new RawProducerConfig(stream) { Reference = "producer" });
            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testPassed.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                });
            var msgData = new Data("apple".AsReadonlySequence());
            var message = new Message(msgData);
            await rawProducer.Send(1, message);
            //wait for sent message to be delivered

            new Utils<Data>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(msgData.Contents.ToArray(), testPassed.Task.Result.Contents.ToArray());
            rawProducer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ProducerAndConsumerCompressShouldHaveTheSameMessages()
        {
            const string UniCode = "Alan Mathison Turing（1912 年 6 月 23 日 - 1954 年 6 月 7 日）是英国数学家";

            void PumpMessages(ICollection<Message> messages, string prefix)
            {
                for (var i = 0; i < 5; i++)
                {
                    messages.Add(new Message(Encoding.UTF8.GetBytes($"{prefix}_{i}"))
                    {
                        ApplicationProperties = new ApplicationProperties()
                        {
                            ["key"] = $"{prefix}_{i}",
                            ["uni"] = $"{UniCode}_{i}",
                            ["float"] = 1_000_000.143,
                            ["int"] = 1_000_000,
                            ["long"] = 1_000_000_000_000,
                            ["bool"] = true
                        },
                        Properties = new Properties()
                        {
                            Subject = null,
                            ReplyTo = null,
                            CorrelationId = null,
                            ContentType = "XML",
                        }
                    });
                }
            }

            void AssertMessages(IReadOnlyList<Message> expected, IReadOnlyList<Message> actual)
            {
                for (var i = 0; i < 5; i++)
                {
                    Assert.Equal(expected[i].Data.Contents.ToArray(), actual[i].Data.Contents.ToArray());
                    Assert.Equal(expected[i].ApplicationProperties["key"], actual[i].ApplicationProperties["key"]);
                    Assert.Equal(expected[i].ApplicationProperties["uni"], actual[i].ApplicationProperties["uni"]);
                    Assert.Equal(expected[i].ApplicationProperties["float"], actual[i].ApplicationProperties["float"]);
                    Assert.Equal(expected[i].ApplicationProperties["int"], actual[i].ApplicationProperties["int"]);
                    Assert.Equal(expected[i].ApplicationProperties["long"], actual[i].ApplicationProperties["long"]);
                    Assert.Equal(expected[i].ApplicationProperties["bool"], actual[i].ApplicationProperties["bool"]);
                    Assert.Equal(expected[i].Properties.Subject, actual[i].Properties.Subject);
                    Assert.Equal(expected[i].Properties.ReplyTo, actual[i].Properties.ReplyTo);
                    Assert.Equal(expected[i].Properties.CorrelationId, actual[i].Properties.CorrelationId);
                    Assert.Equal(expected[i].Properties.ContentType, actual[i].Properties.ContentType);
                }
            }

            var testPassed = new TaskCompletionSource<List<Message>>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));

            var receivedMessages = new List<Message>();
            var messagesInTheChunks = new Dictionary<ulong, uint>();

            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        messagesInTheChunks[ctx.ChunkId] = ctx.ChuckMessagesCount;
                        receivedMessages.Add(message);
                        if (receivedMessages.Count == 10)
                        {
                            testPassed.SetResult(receivedMessages);
                        }

                        await Task.CompletedTask;
                    }
                });

            var rawProducer = await system.CreateRawProducer(
                new RawProducerConfig(stream) { Reference = "producer" });

            var messagesNone = new List<Message>();
            PumpMessages(messagesNone, "None");
            await rawProducer.Send(1, messagesNone, CompressionType.None);

            var messagesGzip = new List<Message>();
            PumpMessages(messagesGzip, "Gzip");
            await rawProducer.Send(2, messagesGzip, CompressionType.Gzip);

            new Utils<List<Message>>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            AssertMessages(messagesNone, testPassed.Task.Result.FindAll(s =>
                Encoding.Default.GetString(s.Data.Contents.ToArray()).Contains("None_")));

            AssertMessages(messagesGzip, testPassed.Task.Result.FindAll(s =>
                Encoding.Default.GetString(s.Data.Contents.ToArray()).Contains("Gzip_")));
            Assert.Equal(10, messagesInTheChunks.Sum(x => x.Value));
            rawProducer.Dispose();
            consumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ConsumerValidationAmqpAttributes()
        {
            // To test large unicode string
            // The Alan Mathison Turing story from wikipedia
            // google translate form English to Chinese ( I don't know chinese )
            const string ChineseStringTest =
                "Alan Mathison Turing（1912 年 6 月 23 日 - 1954 年 6 月 7 日）是英国数学家、计算机科学家、逻辑学家、密码分析家、哲学家和理论生物学家。 [6] 图灵在理论计算机科学的发展中具有很大的影响力，用图灵机提供了算法和计算概念的形式化，可以被认为是通用计算机的模型。[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父。 [10]";

            // 255 string length
            const string ByteString =
                "Alan  Mathison Turing  ( 23 June 1912 – 7 June 1954 ) was an English  mathematician, computer scientist, logician, cryptanalyst,  philosopher, and theoretical biologist. Turing  was   highly  influential in the development of theoretical computer science.";

            var testPassed = new TaskCompletionSource<Message>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var rawProducer = await system.CreateRawProducer(
                new RawProducerConfig(stream) { Reference = "producer" });
            var consumed = 0;
            var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        if (Interlocked.Increment(ref consumed) == 3)
                        {
                            testPassed.SetResult(message);
                        }

                        await Task.CompletedTask;
                    }
                });
            var message = new Message(Encoding.UTF8.GetBytes(ChineseStringTest))
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
                    UserId = new byte[] { 0x0, 0xF },
                    ReplyToGroupId = "replyToGroupId"
                },
                Annotations = new Annotations
                {
                    ["akey1"] = "value1",
                    [1] = 1,
                    [1_000_000] = 1_000_000,
                    ["akey2"] = "value2",
                },
                ApplicationProperties = new ApplicationProperties()
                {
                    ["apkey1"] = "value1",
                    ["apkey2"] = "", //  returns  0x40(Null)  when string is empty or null
                    ["apkey3"] = null, //  returns  0x40(Null)  when string is empty or null 
                    ["keyuni"] = "07-10-2022 午後11:1", //  unicode string,
                    ["keyuni2"] = "良い一日を過ごし、クライアントを楽しんでください", //  unicode string 
                    ["keyuni3"] = "祝您有美好的一天，并享受客户", //  unicode string 
                    ["keylonguni"] = ChineseStringTest, //  unicode string 
                    ["key255"] = ByteString //  unicode string 
                },
            };
            for (ulong i = 0; i < 3; i++)
            {
                await rawProducer.Send(i, message);
            }
            //wait for sent message to be delivered

            new Utils<Message>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            Assert.Equal(ChineseStringTest, Encoding.UTF8.GetString(testPassed.Task.Result.Data.Contents.ToArray()));
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
            Assert.Equal(new byte[] { 0x0, 0xF }, testPassed.Task.Result.Properties.UserId);

            Assert.True(testPassed.Task.Result.Annotations.ContainsKey(1));
            Assert.Equal(1, testPassed.Task.Result.Annotations[1]);
            Assert.Equal("value1", testPassed.Task.Result.Annotations["akey1"]);
            Assert.Equal(1_000_000, testPassed.Task.Result.Annotations[1_000_000]);

            Assert.Equal("value1", testPassed.Task.Result.ApplicationProperties["apkey1"]);
            Assert.Equal("07-10-2022 午後11:1", testPassed.Task.Result.ApplicationProperties["keyuni"]);
            Assert.Equal("良い一日を過ごし、クライアントを楽しんでください", testPassed.Task.Result.ApplicationProperties["keyuni2"]);
            Assert.Equal("祝您有美好的一天，并享受客户", testPassed.Task.Result.ApplicationProperties["keyuni3"]);
            Assert.Equal(ChineseStringTest, testPassed.Task.Result.ApplicationProperties["keylonguni"]);
            Assert.Equal(ByteString, testPassed.Task.Result.ApplicationProperties["key255"]);

            Assert.Null(testPassed.Task.Result.ApplicationProperties["apkey2"]);
            Assert.Null(testPassed.Task.Result.ApplicationProperties["apkey3"]);

            rawProducer.Dispose();
            rawConsumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
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
            var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Crc32 = _crc32,
                    Reference = "consumer",
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

            rawConsumer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ConsumerQueryOffset()
        {
            var testPassed = new TaskCompletionSource<int>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            const int NumberOfMessages = 10;
            const int NumberOfMessagesToStore = 4;
            await SystemUtils.PublishMessages(system, stream, NumberOfMessages, testOutputHelper);
            var count = 0;
            const string Reference = "consumer_offset";
            var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Crc32 = _crc32,
                    Reference = Reference,
                    OffsetSpec = new OffsetTypeOffset(),
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        testOutputHelper.WriteLine($"ConsumerStoreOffset receiving.. {count}");
                        count++;
                        switch (count)
                        {
                            case NumberOfMessagesToStore:
                                // store the the offset after numberOfMessagesToStore messages
                                // so when we query the offset we should (must) have the same
                                // values
                                await consumer.StoreOffset(ctx.Offset);
                                testOutputHelper.WriteLine($"ConsumerStoreOffset done: {count}");
                                break;
                            case NumberOfMessages:
                                testPassed.SetResult(NumberOfMessages);
                                break;
                        }

                        await Task.CompletedTask;
                    }
                });

            new Utils<int>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            // it may need some time to store the offset
            SystemUtils.Wait();
            // numberOfMessagesToStore index 0
            Assert.Equal((ulong)(NumberOfMessagesToStore - 1),
                await system.QueryOffset(Reference, stream));

            // this has to raise OffsetNotFoundException in case the offset 
            // does not exist like in this case.
            await Assert.ThrowsAsync<OffsetNotFoundException>(() =>
                system.QueryOffset("reference_does_not_exist", stream));

            await rawConsumer.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ShouldConsumeFromStoredOffset()
        {
            // validate restart consume offset
            // When a consumer from a stored offset with:
            // await system.QueryOffset(Reference, stream);
            // the user has to receive the messages for that offset.
            // The client receive the chunk this is why we need
            // to filter the value client side
            // see Consumer:MaybeDispatch/1
            // For example given 10 messages in a chunk
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

            var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Crc32 = _crc32,
                    Reference = Reference,
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

            // we need to wait a bit because the StoreOffset is async
            // and `QueryOffset` could raise NoOffsetFound
            SystemUtils.Wait();

            // new consumer that should start from stored offset
            var offset = await system.QueryOffset(Reference, stream);
            // the offset received must be the same from the last stored
            Assert.Equal(offset, storedOffset.Task.Result);
            var messagesConsumed = new TaskCompletionSource<ulong>();
            var rawConsumerWithOffset = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = Reference,
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

            await rawConsumerWithOffset.Close();
            await rawConsumer.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ConsumerMetadataHandlerUpdate()
        {
            // test the Consumer metadata update
            // metadata update can happen when a stream is deleted
            // or when the stream topology is changed.
            // here we test the deleted part
            // with the event: ConsumerConfig:MetadataHandler/1
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var testPassed = new TaskCompletionSource<bool>();

            var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    MetadataHandler = update =>
                    {
                        if (update.Stream == stream)
                        {
                            testPassed.SetResult(true);
                        }

                        return Task.CompletedTask;
                    }
                });
            SystemUtils.Wait();
            await system.DeleteStream(stream);
            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            SystemUtils.WaitUntil(() => ((RawConsumer)rawConsumer).IsOpen() == false);
            await rawConsumer.Close();
            await system.Close();
        }

        [Fact]
        public async void ValidateInitialCredits()
        {
            SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await Consumer.Create(new ConsumerConfig(system, stream) { InitialCredits = 0, }));

            await SystemUtils.CleanUpStreamSystem(system, stream);
        }

        [Fact]
        public async void ProducerConsumerMixingDifferentSendTypesCompressAndStandard()
        {
            // We send messages mixing different send
            // This test is to validate the consumer can handle
            // different messages inside the same chunk
            // This is not a common pattern but it is possible

            var testPassed = new TaskCompletionSource<bool>();

            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            ulong count = 0;
            var rawProducer = await system.CreateRawProducer(
                new RawProducerConfig(stream)
                {
                    ConfirmHandler = conf =>
                    {
                        if (conf.Code == ResponseCode.Ok)
                            return;
                        testOutputHelper.WriteLine($"error confirmation {conf.Code}");
                        testPassed.SetResult(false);
                    }
                });
            var consumer = await system.CreateRawConsumer(new RawConsumerConfig(stream)
            {
                MessageHandler = async (rawConsumer, context, arg3) =>
                {
                    if (Interlocked.Increment(ref count) == 1500)
                    {
                        testPassed.SetResult(true);
                    }

                    await Task.CompletedTask;
                }
            });
            var messages = new List<Message>();
            const int MessagesToSend = 500;
            const int BatchSize = 100;
            ulong pid = 0;
            for (var i = 1; i <= MessagesToSend; i++)
            {
                var msg = new Message(Encoding.UTF8.GetBytes($"data_{i}"));
                messages.Add(msg); // 500
                await rawProducer.Send(pid++, msg);
                if (i % BatchSize == 0)
                {
                    await rawProducer.Send(pid++, messages, CompressionType.None); // 100 * 5
                    await rawProducer.Send(pid++, messages, CompressionType.Gzip); // 100 * 5
                    messages.Clear();
                }
            }

            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            Assert.Equal((ulong)1500, count);
            Assert.Equal(ResponseCode.Ok, await rawProducer.Close());
            await consumer.Close();
            await rawProducer.Close();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ShouldConsumeFromDateTimeOffset()
        {
            // validate the consumer can start from a specific time
            // this test is not deterministic because it depends on the
            // time the test is executed.
            // but at least we can validate the consumer can start from a specific time less 100 ms
            // and it has to receive all the messages
            // not 100% perfect but it is better than nothing

            SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
            var before = DateTimeOffset.Now.AddMilliseconds(-100);
            await SystemUtils.PublishMessages(system, stream, 100, testOutputHelper);
            var testPassed = new TaskCompletionSource<bool>();

            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream)
                {
                    Reference = "consumer",
                    OffsetSpec = new OffsetTypeTimestamp(before),
                    MessageHandler = async (_, ctx, _) =>
                    {
                        Assert.True(ctx.Timestamp >= before.Offset);
                        if (ctx.Offset == 99)
                        {
                            testPassed.SetResult(true);
                        }

                        await Task.CompletedTask;
                    }
                });
            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            await consumer.Close().ConfigureAwait(false);
            await SystemUtils.CleanUpStreamSystem(system, stream);
        }

        [Fact]
        public async void EntityInfoShouldBeCorrect()
        {
            SystemUtils.ResetSuperStreams();
            SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
            var rawConsumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream) { Reference = "consumer", });

            Assert.Equal(stream, rawConsumer.Info.Stream);
            Assert.Equal("consumer", rawConsumer.Info.Reference);
            await rawConsumer.Close();

            var rawProducer = await system.CreateRawProducer(
                new RawProducerConfig(stream) { Reference = "producer", });

            Assert.Equal(stream, rawProducer.Info.Stream);
            Assert.Equal("producer", rawProducer.Info.Reference);
            await rawProducer.Close();

            var rawSuperStreamProducer = await system.CreateRawSuperStreamProducer(
                new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
                {
                    Reference = "super_producer",
                    RoutingStrategyType = RoutingStrategyType.Hash,
                    Routing = _ => "OK",
                });
            Assert.Equal(SystemUtils.InvoicesExchange, rawSuperStreamProducer.Info.Stream);
            Assert.Equal("super_producer", rawSuperStreamProducer.Info.Reference);
            await rawSuperStreamProducer.Close();

            var rawSuperStreamConsumer = await system.CreateSuperStreamConsumer(
                new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange) { Reference = "super_consumer", });

            Assert.Equal(SystemUtils.InvoicesExchange, rawSuperStreamConsumer.Info.Stream);
            Assert.Equal("super_consumer", rawSuperStreamConsumer.Info.Reference);
            await rawSuperStreamConsumer.Close();

            var producer = await Producer.Create(new ProducerConfig(system, stream));

            Assert.Equal(stream, producer.Info.Stream);
            Assert.True(string.IsNullOrWhiteSpace(producer.Info.Reference));
            await producer.Close();

            var consumer = await Consumer.Create(new ConsumerConfig(system, stream) { Reference = "consumer", });

            Assert.Equal(stream, consumer.Info.Stream);
            Assert.Equal("consumer", consumer.Info.Reference);
            await consumer.Close();

            var producerSuperStream =
                await Producer.Create(new ProducerConfig(system, SystemUtils.InvoicesExchange)
                {
                    SuperStreamConfig = new SuperStreamConfig() { Routing = _ => "OK" }
                });

            Assert.Equal(SystemUtils.InvoicesExchange, producerSuperStream.Info.Stream);
            Assert.True(string.IsNullOrWhiteSpace(producerSuperStream.Info.Reference));

            await producerSuperStream.Close();

            var consumerSuperStream = await Consumer.Create(new ConsumerConfig(system, SystemUtils.InvoicesExchange)
            {
                Reference = "consumer",
                IsSuperStream = true,
            });

            Assert.Equal(SystemUtils.InvoicesExchange, consumerSuperStream.Info.Stream);
            Assert.Equal("consumer", consumerSuperStream.Info.Reference);
            await consumerSuperStream.Close();

            var dedProducer =
                await DeduplicatingProducer.Create(new DeduplicatingProducerConfig(system, stream, "dedProducer"));

            Assert.Equal(stream, dedProducer.Info.Stream);
            Assert.Equal("dedProducer", dedProducer.Info.Reference);
            await dedProducer.Close();

            await SystemUtils.CleanUpStreamSystem(system, stream);
        }
    }
}
