﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ClientTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public ClientTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task CreateDeleteStream()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var args = new Dictionary<string, string>();
            var response = await client.CreateStream(stream, args);
            Assert.Equal(ResponseCode.Ok, response.ResponseCode);
            var response2 = await client.CreateStream(stream, args);
            Assert.Equal(ResponseCode.StreamAlreadyExists, response2.ResponseCode); //stream already exists
            var deleteResponse = await client.DeleteStream(stream);
            Assert.Equal(ResponseCode.Ok, deleteResponse.ResponseCode);
            var deleteResponse2 = await client.DeleteStream(stream);
            Assert.Equal(ResponseCode.StreamDoesNotExist, deleteResponse2.ResponseCode);
            await client.Close("done");
        }

        [Fact]
        public async Task MetaDataShouldReturn()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var response = await client.CreateStream(stream, new Dictionary<string, string>());
            Assert.Equal(ResponseCode.Ok, response.ResponseCode);
            var metaDataResponse = await client.QueryMetadata(new[] { stream });
            Assert.Single(metaDataResponse.StreamInfos);
            var streamInfoPair = metaDataResponse.StreamInfos.First();
            Assert.Equal(stream, streamInfoPair.Key);
            var streamInfo = streamInfoPair.Value;
            Assert.Equal(ResponseCode.Ok, streamInfo.ResponseCode);
            Assert.Equal(5552, (int)streamInfo.Leader.Port);
            Assert.Empty(streamInfo.Replicas);

            // Test result when the Stream Does Not Exist
            const string streamNotExist = "StreamNotExist";
            var metaDataResponseNo = await client.QueryMetadata(new[] { streamNotExist });
            Assert.Single(metaDataResponseNo.StreamInfos);
            var streamInfoPairNo = metaDataResponseNo.StreamInfos.First();
            Assert.Equal(streamNotExist, streamInfoPairNo.Key);
            var streamInfoNo = streamInfoPairNo.Value;
            Assert.Equal(ResponseCode.StreamDoesNotExist, streamInfoNo.ResponseCode);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        public async Task MetadataUpdateIsHandled()
        {
            var stream = Guid.NewGuid().ToString();
            var testPassed = new TaskCompletionSource<MetaDataUpdate>();
            var clientParameters = new ClientParameters();
            clientParameters.OnMetadataUpdate += async (update) =>
            {
                testPassed.SetResult(update);
                await Task.CompletedTask;
            };

            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) => { };
            Action<(ulong, ResponseCode)[]> errored = (errors) => { };
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, result) = await client.DeclarePublisher(publisherRef, stream, confirmed, errored);
            Assert.Equal(ResponseCode.Ok, result.ResponseCode);

            await client.DeleteStream(stream);

            await testPassed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var mdu = await testPassed.Task;
            Assert.Equal(stream, mdu.Stream);
            Assert.Equal(ResponseCode.StreamNotAvailable, mdu.Code);
            await client.Close("done");
        }

        [Fact]
        public async Task DeclarePublisherShouldReturnErrorCode()
        {
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<bool>();
            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) => { testPassed.SetResult(false); };
            Action<(ulong, ResponseCode)[]> errored = (errors) => { testPassed.SetResult(true); };
            var publisherRef = Guid.NewGuid().ToString();

            var (publisherId, result) =
                await client.DeclarePublisher(publisherRef, "this-stream-does-not-exist", confirmed, errored,
                    new ConnectionsPool(0, 1, new ConnectionCloseConfig()));
            Assert.Equal(ResponseCode.StreamDoesNotExist, result.ResponseCode);
            await client.Close("done");
        }

        [Fact]
        public async Task DeclareConsumerShouldReturnErrorCode()
        {
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var (subId, subscribeResponse) = await client.Subscribe(
                "this-stream-does-not-exist", new OffsetTypeLast(), 1,
                new Dictionary<string, string>(), null, null, new ConnectionsPool(0, 1, new ConnectionCloseConfig()));
            Assert.Equal(ResponseCode.StreamDoesNotExist, subscribeResponse.ResponseCode);
            await client.Close("done");
        }

        [Fact]
        public async Task PublishShouldError()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            var testPassed = new TaskCompletionSource<bool>();

            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) => { testPassed.SetResult(false); };

            Action<(ulong, ResponseCode)[]> errored = (errors) =>
            {
                testOutputHelper.WriteLine($"publish error code ${errors[0]}");
                testPassed.SetResult(true);
            };

            var publisherRef = Guid.NewGuid().ToString();

            var (publisherId, result) = await client.DeclarePublisher(publisherRef, stream, confirmed, errored);
            var delStream = await client.DeleteStream(stream);
            Assert.Equal(ResponseCode.Ok, delStream.ResponseCode);

            var msgData = new Message(Encoding.UTF8.GetBytes("hi"));
            await client.Publish(new Publish(publisherId, new List<(ulong, Message)> { (100, msgData) }));

            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.True(await testPassed.Task);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        public async Task PublishShouldConfirm()
        {
            testOutputHelper.WriteLine("PublishShouldConfirm");
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            var numConfirmed = 0;
            var testPassed = new TaskCompletionSource<bool>();

            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) =>
            {
                numConfirmed = numConfirmed + pubIds.Length;
                if (numConfirmed == 10000)
                {
                    testPassed.SetResult(true);
                }
            };
            Action<(ulong, ResponseCode)[]> errored = (errors) => throw new Exception($"unexpected errors {errors}");

            var (publisherId, declarePubResp) =
                await client.DeclarePublisher("my-publisher", stream, confirmed, errored);
            var queryPublisherResponse = await client.QueryPublisherSequence("my-publisher", stream);
            // Assert.Equal(9999, (int)queryPublisherResponse.Sequence);
            var from = queryPublisherResponse.Sequence + 1;
            var to = from + 10000;

            for (var i = from; i < to; i++)
            {
                var msgData = new Message(Encoding.UTF8.GetBytes(
                    "asdfasdfasdfasdfljasdlfjasdlkfjalsdkfjlasdkjfalsdkjflaskdjflasdjkflkasdjflasdjflaksdjflsakdjflsakdjflasdkjflasdjflaksfdhi"));
                await client.Publish(new Publish(publisherId, new List<(ulong, Message)> { (i, msgData) }));
                if ((int)i - numConfirmed > 1000)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(10));
                }
            }

            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Debug.WriteLine($"num confirmed {numConfirmed}");
            Assert.True(await testPassed.Task);
            await client.DeleteStream(stream);
            var closeResponse = await client.Close("finished");
            Assert.Equal(ResponseCode.Ok, closeResponse.ResponseCode);
            Assert.True(client.IsClosed);
            //Assert.Throws<AggregateException>(() => client.Close("finished").Result);
            await client.Close("done");
        }

        [Fact]
        public async Task ConsumerShouldReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            const int InitialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            var messages = new List<Message>();

            Task DeliverHandler(Deliver deliver)
            {
                var sequenceReader = new SequenceReader<byte>(new ReadOnlySequence<byte>(deliver.Chunk.Data));

                for (var i = 0; i < deliver.Chunk.NumEntries; i++)
                {
                    WireFormatting.ReadUInt32(ref sequenceReader, out var len);
                    var message = Message.From(ref sequenceReader, len);
                    messages.Add(message);
                    if (messages.Count == 2)
                    {
                        testPassed.SetResult(deliver);
                    }
                }

                return Task.CompletedTask;
            }

            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)InitialCredit,
                new Dictionary<string, string>(), DeliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            await client.Publish(new Publish(publisherId,
                new List<(ulong, Message)>
                {
                    (0, new Message(Encoding.UTF8.GetBytes("hi"))), (1, new Message(Encoding.UTF8.GetBytes("hi")))
                }));
            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(2, messages.Count);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        public async Task ConsumerStoreOffsetShouldReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var publisherRef = Guid.NewGuid().ToString();
            var messageCount = 0;
            ulong offset = 0;
            const string reference = "ref";

            // create stream
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var gotEvent = new ManualResetEvent(false);

            await client.CreateStream(stream, new Dictionary<string, string>());

            // Subscribe
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            var deliverHandler = new Func<Deliver, Task>((Deliver deliver) =>
            {
                var sequenceReader = new SequenceReader<byte>(new ReadOnlySequence<byte>(deliver.Chunk.Data));
                for (ulong i = 0; i < deliver.Chunk.NumEntries; i++)
                {
                    WireFormatting.ReadUInt32(ref sequenceReader, out var len);
                    var message = Message.From(ref sequenceReader, len);
                    message.MessageOffset = deliver.Chunk.ChunkId + i;
                    if (message.MessageOffset >= offset) //a chunk may contain messages before offset
                    {
                        Interlocked.Increment(ref messageCount);
                    }

                    testOutputHelper.WriteLine("GotDelivery: {0}", messageCount);

                    if (messageCount == 10)
                    {
                        testOutputHelper.WriteLine("Got 10: ");
                        gotEvent.Set();
                    }
                }

                client.Credit(deliver.SubscriptionId, 1).AsTask();
                return Task.CompletedTask;
            });
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            testOutputHelper.WriteLine("Initiated new subscriber with id: {0}", subId);

            // publish
            var (publisherId, _) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            for (ulong i = 0; i < 10; i++)
            {
                await client.Publish(new Publish(publisherId,
                    new List<(ulong, Message)> { (i, new Message(Encoding.UTF8.GetBytes("hi"))) }));
            }

            var deletePublisher = await client.DeletePublisher(publisherId);
            Assert.Equal(ResponseCode.Ok, deletePublisher.ResponseCode);
            testOutputHelper.WriteLine("Sent 10 messages to stream");
            if (!gotEvent.WaitOne(TimeSpan.FromSeconds(10)))
            {
                Assert.Fail("MessageHandler was not hit");
            }

            Assert.Equal(10, messageCount);

            await client.StoreOffset(reference, stream, 5);

            // reset
            gotEvent.Reset();
            messageCount = 5;

            var queryOffsetResponse = await client.QueryOffset(reference, stream);
            offset = queryOffsetResponse.Offset;
            testOutputHelper.WriteLine("Current offset for {0}: {1}", reference, offset);
            Assert.Equal((ulong)5, offset);

            var offsetTypeOffset = new OffsetTypeOffset(offset);
            (subId, subscribeResponse) = await client.Subscribe(stream, offsetTypeOffset, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            testOutputHelper.WriteLine("Initiated new subscriber with id: {0}", subId);

            if (!gotEvent.WaitOne(TimeSpan.FromSeconds(10)))
            {
                Assert.Fail("MessageHandler was not hit");
            }

            Assert.Equal(10, messageCount);
            await client.Unsubscribe(subId);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        public async Task ConsumerShouldReceiveDeliveryAfterCredit()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            const int InitialCredit = 0; //no initial credit
            var offsetType = new OffsetTypeFirst();

            Task DeliverHandler(Deliver deliver)
            {
                testPassed.SetResult(deliver);
                return Task.CompletedTask;
            }

            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)InitialCredit,
                new Dictionary<string, string>(), DeliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            await client.Publish(new Publish(publisherId,
                new List<(ulong, Message)> { (0, new Message(Array.Empty<byte>())) }));
            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed, false);

            //We have not credited yet
            await client.Credit(subId, 1);

            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            var delivery = await testPassed.Task;
            Assert.Equal(1, delivery.Chunk.NumEntries);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        public async Task UnsubscribedConsumerShouldNotReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            Func<Deliver, Task> deliverHandler = deliver =>
            {
                testPassed.SetResult(deliver);
                return Task.CompletedTask;
            };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);

            var unsubscribeResponse = await client.Unsubscribe(subId);

            Assert.Equal(ResponseCode.Ok, unsubscribeResponse.ResponseCode);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, _) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            await client.Publish(new Publish(publisherId,
                new List<(ulong, Message)> { (0, new Message(Array.Empty<byte>())) }));
            // 1s should be enough to catch this at least some of the time
            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed, false);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        public async Task VirtualHostFailureAccess()
        {
            var clientParameters = new ClientParameters { VirtualHost = "DOES_NOT_EXIST" };
            await Assert.ThrowsAsync<VirtualHostAccessFailureException>(
                async () => { await Client.Create(clientParameters); }
            );
        }

        [Fact]
        public async Task ExchangeVersionCommandsShouldNotBeEmpty()
        {
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var response = await client.ExchangeVersions();
            Assert.Equal(ResponseCode.Ok, response.ResponseCode);
            Assert.True(response.Commands.Count > 0);
            await client.Close("done");
        }

        [Fact]
        public async Task CreateDeleteSuperStreamWith2Partitions()
        {
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            const string SuperStream = "my_super_stream_with_2_partitions";
            var partitions = new List<string> { "partition_0", "partition_1" };
            var bindingKeys = new List<string>() { "0", "1" };
            var args = new Dictionary<string, string> { { "queue-leader-locator", "least-leaders"},
                { "max-length-bytes", "10000"}, {"max-age", "30000s"}};
            var response = await client.CreateSuperStream(SuperStream, partitions, bindingKeys, args);
            Assert.Equal(ResponseCode.Ok, response.ResponseCode);
            await SystemUtils.WaitAsync(TimeSpan.FromSeconds(1));
            var responseError = await client.CreateSuperStream(SuperStream, partitions, bindingKeys, args);
            Assert.Equal(ResponseCode.StreamAlreadyExists, responseError.ResponseCode);
            var responseDelete = await client.DeleteSuperStream(SuperStream);
            Assert.Equal(ResponseCode.Ok, responseDelete.ResponseCode);
            await SystemUtils.WaitAsync(TimeSpan.FromSeconds(1));
            var responseDeleteError = await client.DeleteSuperStream(SuperStream);
            Assert.Equal(ResponseCode.StreamDoesNotExist, responseDeleteError.ResponseCode);
            await client.Close("done");
        }
    }
}
