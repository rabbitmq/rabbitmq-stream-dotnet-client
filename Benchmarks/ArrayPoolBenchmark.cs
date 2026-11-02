// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(RuntimeMoniker.Net80)]
    public class ArrayPoolBenchmark : RabbitMQStreamBenchmark
    {
        [Params(10, 100, 1_000, 10_000)]
        public int MessagesCount;

        private Producer _producer;
        private static readonly byte[] s_messageData = new byte[10_000];

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            await OneTimeSetUp().ConfigureAwait(false);

            var streamSystemConfig = new StreamSystemConfig()
            {
                Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, _container.GetMappedPublicPort(5552)) }
            };
            var system = await StreamSystem.Create(streamSystemConfig).ConfigureAwait(false);
            const string StreamName = "array-pool-benchmark-stream";
            await system.CreateStream(new StreamSpec(StreamName)).ConfigureAwait(false);

            var producerConfig = new ProducerConfig(system, StreamName);
            _producer = await Producer.Create(producerConfig).ConfigureAwait(false);
        }

        [GlobalCleanup]
        public async Task GlobalCleanup()
        {
            await OneTimeTearDown().ConfigureAwait(false);
        }

        [Benchmark(Baseline = true, Description = "ArrayPool")]
        public async Task GetArrayFromPoolAsync()
        {
            for (var i = 0; i < MessagesCount; i++)
            {
                await _producer.Send(new Message(s_messageData)).ConfigureAwait(false);
            }
        }
    }
}
