using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
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
    public static class TestExtensions
    {
        public static ReadOnlySequence<byte> AsReadonlySequence(this string s)
        {
            return new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(s));
        }
    }
    public class SystemTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public SystemTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void CreateSystem()
        {
            var config = new StreamSystemConfig
            {
                Endpoints = new List<EndPoint>
                {
                    new IPEndPoint(IPAddress.Loopback, 9999), // this should not be bound
                    new IPEndPoint(IPAddress.Loopback, 5552),
                }
            };
            var system = await StreamSystem.Create(config);
            Assert.False(system.IsClosed);
            await system.Close();
            Assert.True(system.IsClosed);
        }
        
        [Fact]
        public async void CreateSystemThrowsWhenNoEndpointsAreReachable()
        {
            var config = new StreamSystemConfig
            {
                Endpoints = new List<EndPoint>
                {
                    new IPEndPoint(IPAddress.Loopback, 9999) // this should not be bound
                }
            };
            await Assert.ThrowsAsync<StreamSystemInitialisationException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async void CreateStream()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            var spec = new StreamSpec(stream)
            {
                MaxAge = TimeSpan.FromHours(8),
                LeaderLocator = LeaderLocator.Random
            };
            Assert.Equal("28800s", spec.Args["max-age"]);
            Assert.Equal("random", spec.Args["queue-leader-locator"]);
            await system.CreateStream(spec);
            await system.Close();
        }
        
        [Fact]
        public async void CreateProducer()
        {
            var testPassed = new TaskCompletionSource<bool>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig{
                    Reference = "producer",
                    Stream = stream,
                    ConfirmHandler = conf =>
                    {
                        if(conf.Code == ResponseCode.Ok)
                            testPassed.SetResult(true);
                        else
                            testPassed.SetResult(false);
                    }
                });
            var readonlySequence = "apple".AsReadonlySequence();
            var message = new Message(new Data(readonlySequence));
            await producer.Send(1, message);
            Assert.True(testPassed.Task.Wait(5000));
            Assert.True(testPassed.Task.Result);
            await system.Close();
        }
        
        [Fact]
        public async void CreateConsumer()
        {
            var testPassed = new TaskCompletionSource<Data>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig{
                    Reference = "producer",
                    Stream = stream
                });
            var consumer = await system.CreateConsumer(
                new ConsumerConfig
                {
                    Reference = "consumer",
                    Stream = stream,
                    MessageHandler = async (consumer, ctx, message) =>
                    {
                        //consumer.Commit(ctx.Offset);
                        testPassed.SetResult(message.Data);
                        await Task.CompletedTask;
                    }
                });
            var msgData = new Data("apple".AsReadonlySequence());
            var message = new Message(msgData);
            await producer.Send(1, message);
            //wait for sent message to be delivered
            Assert.True(testPassed.Task.Wait(5000));
            Assert.Equal(msgData.Contents.ToArray(), testPassed.Task.Result.Contents.ToArray());
            await system.Close();
        }
    }

}