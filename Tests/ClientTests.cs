using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests
{



    [Collection("Sequential")]
    public class ClientTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public ClientTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        
        [Fact]
        [WaitTestBeforeAfter]
        public async void CreateDeleteStream()
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
        [WaitTestBeforeAfter]
        public async void MetaDataShouldReturn()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var response = await client.CreateStream(stream, new Dictionary<string, string>());
            Assert.Equal(ResponseCode.Ok, response.ResponseCode);
            var metaDataResponse = await client.QueryMetadata(new[] { stream });
            Assert.Equal(1, metaDataResponse.StreamInfos.Count);
            var streamInfoPair = metaDataResponse.StreamInfos.First();
            Assert.Equal(stream, streamInfoPair.Key);
            var streamInfo = streamInfoPair.Value;
            Assert.Equal(ResponseCode.Ok, streamInfo.ResponseCode);
            Assert.Equal(5552, (int)streamInfo.Leader.Port);
            Assert.Empty(streamInfo.Replicas);
            
            // Test result when the Stream Does Not Exist
            const string streamNotExist = "StreamNotExist";
            var metaDataResponseNo = await client.QueryMetadata(new[] { streamNotExist});
            Assert.Equal(1, metaDataResponseNo.StreamInfos.Count);
            var streamInfoPairNo = metaDataResponseNo.StreamInfos.First();
            Assert.Equal(streamNotExist, streamInfoPairNo.Key);
            var streamInfoNo = streamInfoPairNo.Value;
            Assert.Equal(ResponseCode.StreamDoesNotExist, streamInfoNo.ResponseCode);
            
            await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void MetadataUpdateIsHandled()
        {
            var stream = Guid.NewGuid().ToString();
            var testPassed = new TaskCompletionSource<MetaDataUpdate>();
            var clientParameters = new ClientParameters { MetadataHandler = m => testPassed.SetResult(m) };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) => { };
            Action<(ulong, ResponseCode)[]> errored = (errors) => { };
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, result) = await client.DeclarePublisher(publisherRef, stream, confirmed, errored);
            Assert.Equal(ResponseCode.Ok, result.ResponseCode);
            
            await client.DeleteStream(stream);
            Assert.True(testPassed.Task.Wait(5000));
            var mdu = testPassed.Task.Result;
            Assert.Equal(stream, mdu.Stream);
            Assert.Equal(ResponseCode.StreamNotAvailable, mdu.Code);
            await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void DeclarePublisherShouldReturnErrorCode()
        {
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<bool>();
            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) => { testPassed.SetResult(false); };
            Action<(ulong, ResponseCode)[]> errored = (errors) => { testPassed.SetResult(true); };
            var publisherRef = Guid.NewGuid().ToString();

            var (publisherId, result) = await client.DeclarePublisher(publisherRef, "this-stream-does-not-exist", confirmed, errored);
            Assert.Equal(ResponseCode.StreamDoesNotExist, result.ResponseCode);
            await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void PublishShouldError()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            var testPassed = new TaskCompletionSource<bool>();

            Action<ReadOnlyMemory<ulong>> confirmed = (pubIds) =>
            {
                testPassed.SetResult(false);
            };

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

            Assert.True(testPassed.Task.Result);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void PublishShouldConfirm()
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

            var (publisherId, declarePubResp) = await client.DeclarePublisher("my-publisher", stream, confirmed, errored);
            var queryPublisherResponse = await client.QueryPublisherSequence("my-publisher", stream);
            // Assert.Equal(9999, (int)queryPublisherResponse.Sequence);
            var from = queryPublisherResponse.Sequence + 1;
            var to = from + 10000;

            for (var i = from; i < to; i++)
            {
                var msgData = new Message(Encoding.UTF8.GetBytes("asdfasdfasdfasdfljasdlfjasdlkfjalsdkfjlasdkjfalsdkjflaskdjflasdjkflkasdjflasdjflaksdjflsakdjflsakdjflasdkjflasdjflaksfdhi"));
                await client.Publish(new Publish(publisherId, new List<(ulong, Message)> { (i, msgData)}));
                if ((int)i - numConfirmed > 1000)
                    await Task.Delay(10);
            }
            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Debug.WriteLine($"num confirmed {numConfirmed}");
            Assert.True(testPassed.Task.Result);
            await client.DeleteStream(stream);
            var closeResponse = await client.Close("finished");
            Assert.Equal(ResponseCode.Ok, closeResponse.ResponseCode);
            Assert.True(client.IsClosed);
            //Assert.Throws<AggregateException>(() => client.Close("finished").Result);
            await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ConsumerShouldReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            var msgs = new List<MsgEntry>();
            Func<Deliver, Task> deliverHandler = deliver =>
            {
                msgs.AddRange(deliver.Messages);
                if(msgs.Count == 2)
                    testPassed.SetResult(deliver);
                return Task.CompletedTask;
            };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            await client.Publish(new Publish(publisherId, new List<(ulong, Message)>
            {
                (0, new Message(Encoding.UTF8.GetBytes("hi"))),
                (1, new Message(Encoding.UTF8.GetBytes("hi")))
            }));
            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            Assert.Equal(2, msgs.Count());
            await client.DeleteStream(stream);
            await client.Close("done");
        }


        [Fact]
        [WaitTestBeforeAfter]
        public async void ConsumerStoreOffsetShouldReceiveDelivery()
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
            var deliverHandler = new Func<Deliver, Task>(async (Deliver deliver) =>
            {
                foreach (var msg in deliver.Messages)
                {
                    if (msg.Offset >= offset) //a chunk may contain messages before offset
                        messageCount++;
                        
                }

                testOutputHelper.WriteLine("GotDelivery: {0}", deliver.Messages.Count());

                if (messageCount == 10)
                {
                    testOutputHelper.WriteLine("Got 10: ");
                    gotEvent.Set();
                }

                await client.Credit(deliver.SubscriptionId, 1);
            });
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            testOutputHelper.WriteLine("Initiated new subscriber with id: {0}", subId);

            // publish
            var (publisherId, _) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            for (ulong i = 0; i < 10; i++)
            {
                await client.Publish(new Publish(publisherId, new List<(ulong, Message)> { (i, new Message(Encoding.UTF8.GetBytes("hi"))) }));
            }

            var deletePublisher = await client.DeletePublisher(publisherId);
            Assert.Equal(ResponseCode.Ok, deletePublisher.ResponseCode);
            testOutputHelper.WriteLine("Sent 10 messages to stream");
            if (!gotEvent.WaitOne(TimeSpan.FromSeconds(10)))
            {
                Assert.True(false, "MessageHandler was not hit");
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
                Assert.True(false, "MessageHandler was not hit");
            }
            Assert.Equal(10, messageCount);
            await client.Unsubscribe(subId);
           // await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void ConsumerShouldReceiveDeliveryAfterCredit()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 0; //no initial credit
            var offsetType = new OffsetTypeFirst();

            Task DeliverHandler(Deliver deliver)
            {
                testPassed.SetResult(deliver);
                return Task.CompletedTask;
            }

            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), DeliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.ResponseCode);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            await client.Publish(new Publish(publisherId, new List<(ulong, Message)> { (0, new Message(Array.Empty<byte>())) }));
            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed, false);

            //We have not credited yet
            await client.Credit(subId, 1);

            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed);

            var delivery = testPassed.Task.Result;
            Assert.Single(delivery.Messages);
            await client.DeleteStream(stream);
            await client.Close("done");
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void UnsubscribedConsumerShouldNotReceiveDelivery()
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
            await client.Publish(new Publish(publisherId, new List<(ulong, Message)> { (0, new Message(Array.Empty<byte>())) }));
            // 1s should be enough to catch this at least some of the time
            new Utils<Deliver>(testOutputHelper).WaitUntilTaskCompletes(testPassed, false);
            await client.DeleteStream(stream);
            await client.Close("done");
        }


        [Fact]
        [WaitTestBeforeAfter]
        public async void VirtualHostFailureAccess()
        {
            var clientParameters = new ClientParameters
            {
                VirtualHost = "DOES_NOT_EXIST"
            };
            await Assert.ThrowsAsync<VirtualHostAccessFailureException>(
                async () => {  await Client.Create(clientParameters); }
            );
        }
    }
}
