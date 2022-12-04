// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using ConsumerConfig = RabbitMQ.Stream.Client.Reliable.ConsumerConfig;

namespace Tests
{
    public class PermissionTests
    {
        [Fact]
        public async void AccessToStreamWithoutGrantsShouldRaiseErrorTest()
        {
            SystemUtils.HttpPost(
                System.Text.Encoding.Default.GetString(
                    SystemUtils.GetFileContent("definition_test.json")), "definitions");
            // load definition creates users and streams to test the access
            // the user "test" can't access on "no_access_stream"
            const string stream = "no_access_stream";
            var config = new StreamSystemConfig() { Password = "test", UserName = "test", VirtualHost = "/" };
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<CreateProducerException>(
                async () =>
                {
                    await system.CreateRawProducer(
                        new RawProducerConfig(stream) { Reference = "producer" });
                }
            );

            await Assert.ThrowsAsync<CreateProducerException>(
                async () =>
                {
                    await Producer.Create(
                        new ProducerConfig(system, stream));
                }
            );

            Producer producer = null;
            try
            {
                producer = await Producer.Create(
                    new ProducerConfig(system, stream));
            }
            catch (Exception)
            {
                // we already tested the Exception (CreateProducerException)
            }

            // here the reliableProducer must be closed due of the CreateProducerException
            Assert.False(producer != null && producer.IsOpen());

            await Assert.ThrowsAsync<CreateConsumerException>(
                async () =>
                {
                    await system.CreateRawConsumer(
                        new RawConsumerConfig(stream) { Reference = "consumer" });
                }
            );

            await Assert.ThrowsAsync<CreateConsumerException>(
                async () =>
                {
                    await Consumer.Create(
                        new ConsumerConfig(system, stream));
                }
            );

            Consumer consumer = null;
            try
            {
                consumer = await Consumer.Create(
                    new ConsumerConfig(system, stream));
            }
            catch (Exception)
            {
                // we already tested the Exception (CreateConsumerException)
            }

            // here the reliableConsumer must be closed due of the CreateConsumerException
            Assert.False(consumer != null && consumer.IsOpen());

            await system.Close();
        }
    }
}
