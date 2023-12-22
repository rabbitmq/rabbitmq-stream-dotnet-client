// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;

namespace Tests
{
    public class EntitiesStateTests
    {
        [Fact]
        public async void RawProducersShouldRaiseErrorWhenClosed()
        {
            SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
            var rawProducer = await system.CreateRawProducer(new RawProducerConfig(stream));

            Assert.True(rawProducer.IsOpen());
            await rawProducer.Send(1, new Message(new byte[] { 1 }));
            await rawProducer.Close();
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await rawProducer.Send(1, new Message(new byte[] { 1 })));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await rawProducer.Send(new List<(ulong, Message)>()));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await rawProducer.Send(1, new List<Message>(), CompressionType.Gzip));

            var producer = await Producer.Create(new ProducerConfig(system, stream));
            Assert.True(producer.IsOpen());
            await producer.Send(new Message(new byte[] { 1 }));
            await producer.Close();
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await producer.Send(new Message(new byte[] { 1 })));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await producer.Send(new List<Message>()));

            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await producer.Send(new List<Message>(), CompressionType.Gzip));

            await SystemUtils.CleanUpStreamSystem(system, stream);
        }

        [Fact]
        public async void RawSuperStreamProducersShouldRaiseErrorWhenClosed()
        {
            SystemUtils.ResetSuperStreams();
            // Simple send message to super stream
            // We should not have any errors and according to the routing strategy
            // the message should be routed to the correct stream
            var system = await StreamSystem.Create(new StreamSystemConfig());
            var rawSuperStreamProducer =
                await system.CreateRawSuperStreamProducer(new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange)
                {
                    Routing = message1 => message1.Properties.MessageId.ToString(),
                });

            Assert.True(rawSuperStreamProducer.IsOpen());
            for (ulong i = 0; i < 20; i++)
            {
                var message = new Message(Encoding.Default.GetBytes("hello"))
                {
                    Properties = new Properties() { MessageId = $"hello{i}" }
                };
                await rawSuperStreamProducer.Send(i, message);
            }

            await rawSuperStreamProducer.Close();
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await rawSuperStreamProducer.Send(1, new Message(new byte[] { 1 })));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await rawSuperStreamProducer.Send(new List<(ulong, Message)>()));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await rawSuperStreamProducer.Send(1, new List<Message>(), CompressionType.Gzip));

            var producer = await Producer.Create(new ProducerConfig(system, SystemUtils.InvoicesExchange)
            {
                SuperStreamConfig = new SuperStreamConfig()
                {
                    Routing = message1 => message1.Properties.MessageId.ToString(),
                }
            });

            Assert.True(producer.IsOpen());
            for (ulong i = 0; i < 20; i++)
            {
                var message = new Message(Encoding.Default.GetBytes("hello"))
                {
                    Properties = new Properties() { MessageId = $"hello{i}" }
                };
                await producer.Send(message);
            }

            await producer.Close();
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await producer.Send(new Message(new byte[] { 1 })));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await producer.Send(new List<Message>()));
            await Assert.ThrowsAsync<AlreadyClosedException>(async () =>
                await producer.Send(new List<Message>(), CompressionType.Gzip));

            await system.Close();
        }
    }
}
