using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class ConsumerSystemTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public ConsumerSystemTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
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
                new ProducerConfig
                {
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
            await system.Close();
        }

        [Fact]
        public async Task CloseProducerTwoTimesShouldBeOk()
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

            await system.Close();
        }
    }
}