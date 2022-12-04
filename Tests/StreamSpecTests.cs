// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    public class StreamSpecTests
    {
        [Fact]
        public void DefaultStreamSpecMustHaveAtLeastQueueLeaderLocator()
        {
            var actualSpec = new StreamSpec("theStreamName");
            var expectedSpec = new StreamSpec("theStreamName")
            {
                LeaderLocator = LeaderLocator.LeastLeaders
            };
            Assert.Equal(expectedSpec.Args, actualSpec.Args);

        }

        [Fact]
        public void CanOverrideAnyStreamSpecAttributes()
        {
            var spec = new StreamSpec("theStreamName");
            spec.MaxAge = TimeSpan.FromHours(3);
            spec.MaxLengthBytes = 10000;
            spec.LeaderLocator = LeaderLocator.Random; // this is an override because the spec has already a default value

            // can override any settings being set
            spec.MaxAge = TimeSpan.FromHours(5);
            spec.MaxLengthBytes = 20000;

            var expectedSpec = new StreamSpec("theStreamName")
            {
                LeaderLocator = LeaderLocator.Random,
                MaxLengthBytes = 20000,
                MaxAge = TimeSpan.FromHours(5)
            };
            Assert.Equal(expectedSpec.Args, spec.Args);
        }
    }
}
