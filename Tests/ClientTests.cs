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
            Assert.Equal(1, streamInfo.Code);
            Assert.Equal(5552, (int)streamInfo.Leader.Port);
            Assert.Empty(streamInfo.Replicas);
            await client.Close("done");
        }

        [Fact]
        public async void MetadataUpdateIsHandled()
        {
            var stream = Guid.NewGuid().ToString();
            var testPassed = new TaskCompletionSource<MetaDataUpdate>();
            var clientParameters = new ClientParameters { MetadataHandler = m => testPassed.SetResult(m) };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            Action<ulong[]> confirmed = (pubIds) => { };
            Action<(ulong, ushort)[]> errored = (errors) => { };
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, result) = await client.DeclarePublisher(publisherRef, stream, confirmed, errored);
            Assert.Equal(1, result.ResponseCode);
            await client.DeleteStream(stream);
            Assert.True(testPassed.Task.Wait(5000));
            var mdu = testPassed.Task.Result;
            Assert.Equal(stream, mdu.Stream);
            Assert.Equal(ResponseCode.StreamNotAvailable, mdu.Code);
        }

        [Fact]
        public async void DeclarePublisherShouldReturnErrorCode()
        {
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<bool>();
            Action<ulong[]> confirmed = (pubIds) => { testPassed.SetResult(false); };
            Action<(ulong, ushort)[]> errored = (errors) => { testPassed.SetResult(true); };
            var publisherRef = Guid.NewGuid().ToString();

            var (publisherId, result) = await client.DeclarePublisher(publisherRef, "this-stream-does-not-exist", confirmed, errored);
            Assert.Equal(2, result.ResponseCode);
        }

        [Fact]
        public async void PublishShouldError()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            var testPassed = new TaskCompletionSource<bool>();

            Action<ulong[]> confirmed = (pubIds) =>
            {
                testPassed.SetResult(false);
            };

            Action<(ulong, ushort)[]> errored = (errors) =>
            {
                testPassed.SetResult(true);
            };
            var publisherRef = Guid.NewGuid().ToString();

            var (publisherId, result) = await client.DeclarePublisher(publisherRef, stream, confirmed, errored);
            var dpr = await client.DeletePublisher(publisherId);
            Assert.Equal(1, dpr.ResponseCode);

            var msgData = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("hi"));
            client.Publish(new OutgoingMsg(publisherId, 100, msgData));

            Assert.True(testPassed.Task.Wait(5000));
            Assert.True(testPassed.Task.Result);
        }

        [Fact]
        public async void PublishShouldConfirm()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            await client.CreateStream(stream, new Dictionary<string, string>());
            var numConfirmed = 0;
            var testPassed = new TaskCompletionSource<bool>();

            Action<ulong[]> confirmed = (pubIds) =>
            {
                numConfirmed = numConfirmed + pubIds.Length;
                if (numConfirmed == 10000)
                {
                    testPassed.SetResult(true);
                }
            };
            Action<(ulong, ushort)[]> errored = (errors) =>
            {
                throw new Exception($"unexpected errors {errors}");
            };

            var (publisherId, declarePubResp) = await client.DeclarePublisher("my-publisher", stream, confirmed, errored);
            var queryPublisherResponse = await client.QueryPublisherSequence("my-publisher", stream);
            // Assert.Equal(9999, (int)queryPublisherResponse.Sequence);
            var from = queryPublisherResponse.Sequence + 1;
            var to = from + 10000;

            for (var i = from; i < to; i++)
            {
                var msgData = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("asdfasdfasdfasdfljasdlfjasdlkfjalsdkfjlasdkjfalsdkjflaskdjflasdjkflkasdjflasdjflaksdjflsakdjflsakdjflasdkjflasdjflaksfdhi"));
                client.Publish(new OutgoingMsg(publisherId, i, msgData));
                if ((int)i - numConfirmed > 1000)
                    await Task.Delay(10);
            }
            Assert.True(testPassed.Task.Wait(10000));
            Debug.WriteLine($"num confirmed {numConfirmed}");
            Assert.True(testPassed.Task.Result);
            await client.DeleteStream(stream);
            var closeResponse = await client.Close("finished");
            Assert.Equal(1, closeResponse.ResponseCode);
            Assert.True(client.IsClosed);
            Assert.Throws<AggregateException>(() => client.Close("finished").Result);
        }

        [Fact]
        public async void ConsumerShouldReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            Action<Deliver> deliverHandler = deliver => { testPassed.SetResult(deliver); };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            client.Publish(new OutgoingMsg(publisherId, 0, new ReadOnlySequence<byte>()));
            Assert.True(testPassed.Task.Wait(10000));
            var delivery = testPassed.Task.Result;
            Assert.Single(delivery.Messages);
        }


        [Fact]
        public async void ConsumerStoreOffsetShouldReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var publisherRef = Guid.NewGuid().ToString();
            int messageCount = 0;
            ulong offset = 0;
            var reference = "ref";

            // create stream
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var gotEvent = new ManualResetEvent(false);

            await client.CreateStream(stream, new Dictionary<string, string>());


            // Subscribe
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            void DeliverHandler(Deliver deliver)
            {
                foreach (var msg in deliver.Messages)
                {
                    if (msg.Offset >= offset) //a chunk may contain messages before offset
                        messageCount++;
                }
                gotEvent.Set();
            }
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), DeliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);
            testOutputHelper.WriteLine("Initiated new subscriber with id: {0}", subId);

            // publish
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            for (ulong i = 0; i < 10; i++)
            {
                client.Publish(new OutgoingMsg(publisherId, i, new ReadOnlySequence<byte>()));
            }
            testOutputHelper.WriteLine("Sent 10 messages to stream");
            if (!gotEvent.WaitOne(TimeSpan.FromSeconds(5)))
            {
                Assert.True(false, "MessageHandler was not hit");
            }
            Assert.Equal(10, messageCount);

            client.StoreOffset(reference, stream, 5);

            // reset
            gotEvent.Reset();
            messageCount = 0;

            var queryOffsetResponse = await client.QueryOffset(reference, stream);
            offset = queryOffsetResponse.Offset;
            testOutputHelper.WriteLine("Current offset for {0}: {1}", reference, offset);
            Assert.Equal((ulong)5, offset);

            var offsetTypeOffset = new OffsetTypeOffset(offset);
            (subId, subscribeResponse) = await client.Subscribe(stream, offsetTypeOffset, (ushort)initialCredit,
               new Dictionary<string, string>(), DeliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);
            testOutputHelper.WriteLine("Initiated new subscriber with id: {0}", subId);

            if (!gotEvent.WaitOne(TimeSpan.FromSeconds(5)))
            {
                Assert.True(false, "MessageHandler was not hit");
            }
            Assert.Equal(5, messageCount);
        }

        [Fact]
        public async void ConsumerShouldReceiveDeliveryAfterCredit()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 0; //no initial credit
            var offsetType = new OffsetTypeFirst();
            Action<Deliver> deliverHandler = deliver => { testPassed.SetResult(deliver); };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            client.Publish(new OutgoingMsg(publisherId, 0, new ReadOnlySequence<byte>()));
            Assert.False(testPassed.Task.Wait(1000));
            //We have not credited yet
            client.Credit(subId, 1);

            Assert.True(testPassed.Task.Wait(10000));
            var delivery = testPassed.Task.Result;
            Assert.Single(delivery.Messages);
        }

        [Fact]
        public async void UnsubscribedConsumerShouldNotReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters { };
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            Action<Deliver> deliverHandler = deliver => { testPassed.SetResult(deliver); };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort)initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);

            var unsubscribeResponse = await client.Unsubscribe(subId);

            Assert.Equal(ResponseCode.Ok, unsubscribeResponse.Code);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => { }, _ => { });
            client.Publish(new OutgoingMsg(publisherId, 0, new ReadOnlySequence<byte>()));
            // 1s should be enough to catch this at least some of the time
            Assert.False(testPassed.Task.Wait(1000));
        }
    }
}
