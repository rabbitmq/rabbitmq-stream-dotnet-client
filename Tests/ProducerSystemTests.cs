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
    }
}