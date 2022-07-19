// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;

namespace Tests
{
    public class PermissionTests
    {
        private const string StreamName = "no_access_stream";

        private readonly StreamSystemConfig _systemConfig = new()
        {
            Password = "test", UserName = "test", VirtualHost = "/"
        };

        public PermissionTests()
        {
            // load definition creates users and streams to test the access
            // the user "test" can't access on "no_access_stream"
            var contents = SystemUtils.GetFileContent("definition_test.json");
            var encodedString = System.Text.Encoding.Default.GetString(contents);
            SystemUtils.HttpPost(encodedString, "definitions");
        }

        [Fact]
        public async Task AccessToStreamWithoutGrantsShouldRaiseErrorForProducer()
        {
            var system = await StreamSystem.Create(_systemConfig);
            var producerConfig = new ProducerConfig {Reference = "producer", Stream = StreamName};
            await Assert.ThrowsAsync<CreateProducerException>(() => system.CreateProducer(producerConfig));
            await system.Close();
        }

        [Fact]
        public async Task AccessToStreamWithoutGrantsShouldRaiseErrorForConsumer()
        {
            var system = await StreamSystem.Create(_systemConfig);
            var consumerConfig = new ConsumerConfig {Reference = "consumer", Stream = StreamName};
            await Assert.ThrowsAsync<CreateConsumerException>(() => system.CreateConsumer(consumerConfig));
            await system.Close();
        }

        [Fact]
        public async Task AccessToStreamWithoutGrantsShouldRaiseErrorForReliableConsumer()
        {
            var system = await StreamSystem.Create(_systemConfig);
            var rConsumerConfig = new ReliableConsumerConfig {Stream = StreamName, StreamSystem = system};

            await Assert.ThrowsAsync<CreateConsumerException>(() =>
                ReliableConsumer.CreateReliableConsumer(rConsumerConfig));

            ReliableConsumer reliableConsumer = null;
            try
            {
                reliableConsumer = await ReliableConsumer.CreateReliableConsumer(rConsumerConfig);
            }
            catch
            {
                // we already tested the Exception (CreateConsumerException)
            }

            // here the reliableConsumer must be closed due of the CreateConsumerException
            Assert.NotNull(reliableConsumer);
            Assert.False(reliableConsumer.IsOpen);
            await system.Close();
        }

        [Fact]
        public async void AccessToStreamWithoutGrantsShouldRaiseError()
        {
            var system = await StreamSystem.Create(_systemConfig);
            var rProducerConfig = new ReliableProducerConfig {Stream = StreamName, StreamSystem = system};
            await Assert.ThrowsAsync<CreateProducerException>(() =>
                ReliableProducer.CreateReliableProducer(rProducerConfig));

            ReliableProducer reliableProducer = null;
            try
            {
                reliableProducer = await ReliableProducer.CreateReliableProducer(rProducerConfig);
            }
            catch
            {
                // we already tested the Exception (CreateProducerException)
            }

            // here the reliableProducer must be closed due of the CreateProducerException
            Assert.NotNull(reliableProducer);
            Assert.False(reliableProducer.IsOpen);

            await system.Close();
        }
    }
}
