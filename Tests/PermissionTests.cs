// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;

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
                    await system.CreateProducer(
                        new ProducerConfig { Reference = "producer", Stream = stream, });
                }
            );

            await Assert.ThrowsAsync<CreateProducerException>(
                async () =>
                {
                    await ReliableProducer.CreateReliableProducer(
                        new ReliableProducerConfig() { Stream = stream, StreamSystem = system });
                }
            );

            ReliableProducer reliableProducer = null;
            try
            {
                reliableProducer = await ReliableProducer.CreateReliableProducer(
                    new ReliableProducerConfig() { Stream = stream, StreamSystem = system });
            }
            catch (Exception)
            {
                // we already tested the Exception (CreateProducerException)
            }
            // here the reliableProducer must be closed due of the CreateProducerException
            Assert.False(reliableProducer != null && reliableProducer.IsOpen());

            await Assert.ThrowsAsync<CreateConsumerException>(
                async () =>
                {
                    await system.CreateConsumer(
                        new ConsumerConfig() { Reference = "consumer", Stream = stream, });
                }
            );

            await Assert.ThrowsAsync<CreateConsumerException>(
                async () =>
                {
                    await ReliableConsumer.CreateReliableConsumer(
                        new ReliableConsumerConfig() { Stream = stream, StreamSystem = system });
                }
            );

            ReliableConsumer reliableConsumer = null;
            try
            {
                reliableConsumer = await ReliableConsumer.CreateReliableConsumer(
                    new ReliableConsumerConfig() { Stream = stream, StreamSystem = system });
            }
            catch (Exception)
            {
                // we already tested the Exception (CreateConsumerException)
            }

            // here the reliableConsumer must be closed due of the CreateConsumerException
            Assert.False(reliableConsumer != null && reliableConsumer.IsOpen());

            await system.Close();
        }
    }
}
