// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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

    public record SuperStreamSpec(string Name)
    {
        internal void Validate()
        {
            if (!AvailableFeaturesSingleton.Instance.Is313OrMore)
            {
                throw new UnsupportedOperationException(Consts.SuperStreamCreationNotSupported);
            }

            if (Partitions < 1)
            {
                throw new ArgumentException("Partitions must be at least 1");
            }

            if (BindingKeys != null && BindingKeys.Count != Partitions)
            {
                throw new ArgumentException("BindingKeys must be null or have the same number of elements as Partitions");
            }
        }

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

        public int Partitions { get; set; } = 3;

        internal List<string> GetPartitions()
        {
            var partitions = new List<string>();
            for (var i = 0; i < Partitions; i++)
            {
                partitions.Add($"{Name}-{i}");
            }

            return partitions;
        }

        public List<string> BindingKeys { get; set; } = null;

        internal List<string> GetBindingKeys()
        {
            if (BindingKeys != null)
            {
                return BindingKeys;
            }

            var bindingKeys = new List<string>();
            for (var i = 0; i < Partitions; i++)
            {
                bindingKeys.Add($"{i}");
            }

            return bindingKeys;
        }

        public IDictionary<string, string> Args => args;
    }
}
