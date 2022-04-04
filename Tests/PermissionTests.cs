// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using RabbitMQ.Stream.Client;
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
            var config = new StreamSystemConfig()
            {
                Password = "test",
                UserName = "test",
                VirtualHost = "/"
            };
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<CreateProducerException>(
                async () =>
                {
                    await system.CreateProducer(
                        new ProducerConfig
                        {
                            Reference = "producer",
                            Stream = stream,
                        });
                }
            );

            await Assert.ThrowsAsync<CreateConsumerException>(
                async () =>
                {
                    await system.CreateConsumer(
                        new ConsumerConfig()
                        {
                            Reference = "consumer",
                            Stream = stream,
                        });
                }
            );

            await system.Close();
        }
    }
}
