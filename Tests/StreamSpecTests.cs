// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
            var spec = new StreamSpec("theStreamName")
            {
                MaxAge = TimeSpan.FromHours(3),
                MaxLengthBytes = 10000,
                LeaderLocator = LeaderLocator.Random // this is an override because the spec has already a default value
            };

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

        [Fact]
        public void MaxLengthAttributeCanContainUInt64ValuesRange()
        {
            var spec = new StreamSpec("theStreamName")
            {
                MaxLengthBytes = ulong.MaxValue
            };

            Assert.Equal(ulong.MaxValue.ToString(), $"{spec.Args["max-length-bytes"]}");
        }

        [Theory]
        [InlineData(0UL)]
        [InlineData(1UL)]
        [InlineData(1_000_000UL)]
        [InlineData(StreamSpec.MaxInitialOffset)]
        public void InitialOffsetIsStoredAsStreamArgument(ulong offset)
        {
            var spec = new StreamSpec("theStreamName") { InitialOffset = offset };
            Assert.Equal(offset.ToString(), spec.Args["stream-initial-offset"]);
        }

        [Theory]
        [InlineData(StreamSpec.MaxInitialOffset + 1)]
        [InlineData(ulong.MaxValue)]
        public void InitialOffsetOutOfRangeIsRejected(ulong offset)
        {
            var spec = new StreamSpec("theStreamName");
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() => spec.InitialOffset = offset);
            Assert.Contains($"Initial offset must be between 0 and {StreamSpec.MaxInitialOffset}", ex.Message);
            Assert.False(spec.Args.ContainsKey("stream-initial-offset"));
        }
    }
}
