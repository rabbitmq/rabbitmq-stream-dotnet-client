﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public record StreamSpec(string Name)
    {
        private readonly IDictionary<string, string> args = new Dictionary<string, string>()
        {
            ["queue-leader-locator"] = LeaderLocator.LeastLeaders.ToString()
        };

        public TimeSpan MaxAge
        {
            set => Args["max-age"] = $"{value.TotalSeconds}s";
        }

        public ulong MaxLengthBytes
        {
            set => Args["max-length-bytes"] = $"{value}";
        }

        public LeaderLocator LeaderLocator
        {
            set => Args["queue-leader-locator"] = $"{value.ToString()}";
        }
        public int MaxSegmentSizeBytes
        {
            set => Args["stream-max-segment-size-bytes"] = $"{value}";
        }

        public IDictionary<string, string> Args => args;
    }
}
