using System;
using System.Buffers;
using System.Collections.Generic;
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
    public class ProducerSystemTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public ProducerSystemTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async void CreateProducer()
        {
            var testPassed = new TaskCompletionSource<bool>();
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                    ConfirmHandler = conf =>
                    {
                        testOutputHelper.WriteLine($"CreateProducer Confirm Handler #{conf.Code}");
                        testPassed.SetResult(conf.Code == ResponseCode.Ok);
                    }
                });

            var readonlySequence = "apple".AsReadonlySequence();
            var message = new Message(new Data(readonlySequence));
            await producer.Send(1, message);


            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);


            Assert.True(testPassed.Task.Result);
            producer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async Task CreateProducerStreamDoesNotExist()
        {
            const string stream = "StreamNotExist";
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<CreateProducerException>(() => system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                }));

            await system.Close();
        }

        [Fact]
        [WaitTestBeforeAfter]
        public async Task CloseProducerTwoTimesShouldReturnOk()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                });

            Assert.Equal(ResponseCode.Ok, await producer.Close());
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            producer.Dispose();
            await system.DeleteStream(stream);
            await system.Close();
        }


        [Fact]
        [WaitTestBeforeAfter]
        public async void NotifyProducerClose()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var testPassed = new TaskCompletionSource<bool>();
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                    ConnectionClosedHandler = async s =>
                    {
                        testOutputHelper.WriteLine("NotifyProducerClose true");
                        testPassed.SetResult(true);
                        await Task.CompletedTask;
                    }
                });


            Assert.Equal(ResponseCode.Ok, await producer.Close());
            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            await system.DeleteStream(stream);
            await system.Close();
        }


        [Fact]
        [WaitTestBeforeAfter]
        public async void ProducerMessagesListLenValidation()
        {
            // by protocol the subEntryList is ushort
            var messages = new List<Message>();
            for (var i = 0; i < (ushort.MaxValue + 1); i++)
            {
                messages.Add(new Message(Encoding.UTF8.GetBytes($"data_{i}")));
            }

            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                });

            await Assert.ThrowsAsync<OutOfBoundsException>(() =>
                producer.Send(1, messages, CompressionType.Gzip).AsTask());

            Assert.Equal(ResponseCode.Ok, await producer.Close());
            await system.DeleteStream(stream);
            await system.Close();
        }


        [Fact]
        [WaitTestBeforeAfter]
        public async void ProducerMixingDifferentConfirmations()
        {
            // we send 150 messages using three different ways:
            // 1- 50 messages subEntry with compress None 
            // 2- 50 messages with standard send 
            // 3- subEntry with compress Gzip
            // we should receive only 52 conformations
            // one for the point 1
            // 50 for the point 2
            // one for the point 3
            var messages = new List<Message>();
            for (var i = 0; i < 50; i++)
            {
                messages.Add(new Message(Encoding.UTF8.GetBytes($"sub{i}")));
            }

            var testPassed = new TaskCompletionSource<bool>();

            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            ulong count = 0;
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                    ConfirmHandler = confirmation =>
                    {
                        count = Interlocked.Increment(ref count);
                        if (count == 52)
                        {
                            testPassed.SetResult(true);
                        }
                    }
                });

            ulong pid = 0;
            await producer.Send(++pid, messages, CompressionType.None);

            foreach (var message in messages)
            {
                await producer.Send(++pid, message);
            }

            await producer.Send(++pid, messages, CompressionType.Gzip);
            Thread.Sleep(500);
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            new Utils<bool>(testOutputHelper).WaitUntilTaskCompletes(testPassed);
            Assert.Equal((ulong) 52, count);
            await system.DeleteStream(stream);
            await system.Close();
        }
    }
}