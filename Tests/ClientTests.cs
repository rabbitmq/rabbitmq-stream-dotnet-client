using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using NuGet.Frameworks;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    public class ClientTests
    {
        [Fact]
        public async void CreateDeleteStream()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters{};
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
            var stream = Guid.NewGuid();
            var clientParameters = new ClientParameters{};
            var client = await Client.Create(clientParameters);
            var metaDataResponse = await client.QueryMetadata(new []{"s1"});
            Assert.Equal(1, metaDataResponse.StreamInfos.Count);
            var streamInfo = metaDataResponse.StreamInfos.First();
            Assert.Equal("s1", streamInfo.Key);
            var s1 = streamInfo.Value;
            Assert.Equal(1, s1.Code);
            Assert.Equal(5552, (int)s1.Leader.Port);
            Assert.Empty(s1.Replicas);
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
            var clientParameters = new ClientParameters{};
            var client = await Client.Create(clientParameters);
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

            var (publisherId, result) = await client.DeclarePublisher(publisherRef, "s1", confirmed, errored);
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
            var clientParameters = new ClientParameters{};
            var client = await Client.Create(clientParameters);
            var numConfirmed = 0;
            var testPassed = new TaskCompletionSource<bool>();

            Action<ulong[]> confirmed = (pubIds) =>
            {
                numConfirmed = numConfirmed + pubIds.Length;
                if(numConfirmed == 10000)
                {
                    testPassed.SetResult(true);
                }
            };
            Action<(ulong, ushort)[]> errored = (errors) =>
            {
                throw new Exception($"unexpected errors {errors}");
            };

            var (publisherId, declarePubResp) = await client.DeclarePublisher("my-publisher", "s1", confirmed, errored);
            var queryPublisherResponse = await client.QueryPublisherSequence("my-publisher", "s1");
            // Assert.Equal(9999, (int)queryPublisherResponse.Sequence);
            var from = queryPublisherResponse.Sequence + 1;
            var to = from + 10000;
            
            for (var i = from; i < to; i++)
            {
                var msgData = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("asdfasdfasdfasdfljasdlfjasdlkfjalsdkfjlasdkjfalsdkjflaskdjflasdjkflkasdjflasdjflaksdjflsakdjflsakdjflasdkjflasdjflaksfdhi"));
                client.Publish(new OutgoingMsg(publisherId, i, msgData));
                if((int)i - numConfirmed > 1000)
                    await Task.Delay(10);
            }
            Assert.True(testPassed.Task.Wait(10000));
            Debug.WriteLine($"num confirmed {numConfirmed}");
            Assert.True(testPassed.Task.Result);
            var closeResponse = await client.Close("finished");
            Assert.Equal(1, closeResponse.ResponseCode);
            Assert.True(client.IsClosed);
            Assert.Throws<AggregateException>(() => client.Close("finished").Result);
        }
        
        [Fact]
        public async void ConsumerShouldReceiveDelivery()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters{};
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 1;
            var offsetType = new OffsetTypeFirst();
            Action<Deliver> deliverHandler = deliver => { testPassed.SetResult(deliver); };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort) initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => {}, _ => {});
            client.Publish(new OutgoingMsg(publisherId, 0, new ReadOnlySequence<byte>()));
            Assert.True(testPassed.Task.Wait(10000));
            var delivery = testPassed.Task.Result;
            Assert.Single(delivery.Messages);
        }
        
        [Fact]
        public async void ConsumerShouldReceiveDeliveryAfterCredit()
        {
            var stream = Guid.NewGuid().ToString();
            var clientParameters = new ClientParameters{};
            var client = await Client.Create(clientParameters);
            var testPassed = new TaskCompletionSource<Deliver>();
            await client.CreateStream(stream, new Dictionary<string, string>());
            var initialCredit = 0; //no initial credit
            var offsetType = new OffsetTypeFirst();
            Action<Deliver> deliverHandler = deliver => { testPassed.SetResult(deliver); };
            var (subId, subscribeResponse) = await client.Subscribe(stream, offsetType, (ushort) initialCredit,
                new Dictionary<string, string>(), deliverHandler);
            Assert.Equal(ResponseCode.Ok, subscribeResponse.Code);
            var publisherRef = Guid.NewGuid().ToString();
            var (publisherId, declarePubResp) = await client.DeclarePublisher(publisherRef, stream, _ => {}, _ => {});
            client.Publish(new OutgoingMsg(publisherId, 0, new ReadOnlySequence<byte>()));
            Assert.False(testPassed.Task.Wait(1000));
            //We have not credited yet
            client.Credit(subId, 1);
           
            Assert.True(testPassed.Task.Wait(10000));
            var delivery = testPassed.Task.Result;
            Assert.Single(delivery.Messages);
        }
    }
}
