// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;

namespace Tests;

public class UsabilityTests
{
    [Fact]
    public async Task AssignLogger()
    {
        var factory = new NullLoggerFactory();

        var producerLogger = factory.CreateLogger<Producer>();
        var consumerLogger = factory.CreateLogger<Consumer>();

        var streamSystemConfig = new StreamSystemConfig();
        var system = await StreamSystem.Create(streamSystemConfig);
        const string StreamName = "logger-test-stream";
        await system.CreateStream(new StreamSpec(StreamName));

        // Creating normal consumer/producer
        var producerConfig = new ProducerConfig(system, StreamName);
        var consumerConfig = new ConsumerConfig(system, StreamName);
        var producer = await Producer.Create(producerConfig, producerLogger);
        var consumer = await Consumer.Create(consumerConfig, consumerLogger);

        var rawConsumerLogger = factory.CreateLogger<RawConsumer>();
        var rawConsumer = await system.CreateRawConsumer(new RawConsumerConfig(StreamName), rawConsumerLogger);

        var rawProducerLogger = factory.CreateLogger<RawProducer>();
        var rawProducer = await system.CreateRawProducer(new RawProducerConfig(StreamName), rawProducerLogger);

        SystemUtils.ResetSuperStreams();
        var rawSuperProducerLogger = factory.CreateLogger<RawSuperStreamProducer>();
        var rawSuperConsumerLogger = factory.CreateLogger<RawSuperStreamConsumer>();
        var rawSuperProducer = await system.CreateRawSuperStreamProducer(
            new RawSuperStreamProducerConfig(SystemUtils.InvoicesExchange) { Routing = message => "1" },
            rawSuperProducerLogger);
        var rawSuperConsumer =
            await system.CreateSuperStreamConsumer(new RawSuperStreamConsumerConfig(SystemUtils.InvoicesExchange),
                rawSuperConsumerLogger);

        await producer.Close();
        await consumer.Close();
        await rawConsumer.Close();
        await rawProducer.Close();
        await rawSuperProducer.Close();
        await rawSuperConsumer.Close();
    }
}
