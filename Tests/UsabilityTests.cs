// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Collections.Generic;
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
    public async Task LetsSee()
    {
        var factory = new NullLoggerFactory();
        
        var producerLogger = factory.CreateLogger<Producer>();
        var consumerLogger = factory.CreateLogger<Consumer>();
        var rawProducerLogger = factory.CreateLogger<RawProducer>();
        var rawConsumerLogger = factory.CreateLogger<RawConsumer>();
        var superStreamConsumerLogger = factory.CreateLogger<SuperStreamConsumer>();
        var rawSuperStreamProducerLogger = factory.CreateLogger<RawSuperStreamProducer>();

        var streamSystemConfig = new StreamSystemConfig();
        var streamSystem = await StreamSystem.Create(streamSystemConfig);

        // Creating raw consumer/producer
        var clientParameters = new ClientParameters();
        var rawProducerConfig = new RawProducerConfig("");
        var rawConsumerConfig = new RawConsumerConfig("");
        
        var rawProducer = await RawProducer.Create(clientParameters, rawProducerConfig, new StreamInfo(), rawProducerLogger);
        var rawConsumer = await RawConsumer.Create(clientParameters, rawConsumerConfig, new StreamInfo(), rawConsumerLogger);
        
        // Creating normal consumer/producer
        var producerConfig = new ProducerConfig(streamSystem, "");
        var consumerConfig = new ConsumerConfig(streamSystem, "");
        var producer = await Producer.Create(producerConfig, producerLogger);
        var consumer = await Consumer.Create(consumerConfig, consumerLogger);
        
        // Creating super consumer/producer
        var superConsumerConfig = new SuperStreamConsumerConfig();
        var superProducerConfig = new RawSuperStreamProducerConfig("");
        var superStreamConsumer = SuperStreamConsumer.Create(superConsumerConfig, new Dictionary<string, StreamInfo>(), clientParameters, superStreamConsumerLogger);
        var superStreamProducer = RawSuperStreamProducer.Create(superProducerConfig, new Dictionary<string, StreamInfo>(), clientParameters, rawSuperStreamProducerLogger);
    }
}
