﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Linq;

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

    /// <summary>
    /// Abstract class for SuperStreamSpec
    /// </summary>
    /// <param name="Name"> Super Stream Name</param>
    public abstract record SuperStreamSpec(string Name)
    {
        internal virtual void Validate()
        {
            if (!AvailableFeaturesSingleton.Instance.Is313OrMore)
            {
                throw new UnsupportedOperationException(Consts.SuperStreamCreationNotSupported);
            }
        }

        internal abstract List<string> GetPartitions();
        internal abstract List<string> GetBindingKeys();

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

    /// <summary>
    /// Create a super stream based on the number of partitions.
    /// So there will be N partitions and N binding keys.
    /// The stream names is the super stream name with a partition number appended.
    /// The routing key is the partition number.
    /// Producer should use HASH strategy to route the message to the correct partition.
    /// Partitions should be at least 1.
    /// </summary>
    public record PartitionsSuperStreamSpec : SuperStreamSpec
    {

        public PartitionsSuperStreamSpec(string Name) : base(Name)
        {
            Partitions = 3;
        }

        public PartitionsSuperStreamSpec(string Name, int partitions) : base(Name)
        {
            Partitions = partitions;
        }

        internal override void Validate()
        {
            base.Validate();
            if (Partitions < 1)
            {
                throw new ArgumentException("Partitions must be at least 1");
            }
        }

        internal override List<string> GetPartitions()
        {
            var partitions = new List<string>();
            for (var i = 0; i < Partitions; i++)
            {
                partitions.Add($"{Name}-{i}");
            }

            return partitions;
        }

        internal override List<string> GetBindingKeys()
        {
            var bindingKeys = new List<string>();
            for (var i = 0; i < Partitions; i++)
            {
                bindingKeys.Add($"{i}");
            }

            return bindingKeys;
        }
        public int Partitions { get; } = 3;

    }

    /// <summary>
    /// Create a super stream based on the number of binding keys.
    /// So there will be N partitions and N binding keys.
    /// The stream names is the super stream name with a binding key appended.
    /// Producer should use KEY strategy to route the message to the correct partition.
    /// The binding keys should be unique duplicates are not allowed.
    /// </summary>
    public record BindingsSuperStreamSpec : SuperStreamSpec
    {
        public BindingsSuperStreamSpec(string Name, string[] bindingKeys) : base(Name)
        {
            BindingKeys = bindingKeys;
        }

        internal override void Validate()
        {
            base.Validate();
            if (BindingKeys == null || !BindingKeys.Any())
            {
                throw new ArgumentException("Bindings must be at least 1");
            }

            if (BindingKeys.GroupBy(x => x).Any(g => g.Count() > 1))
            {
                throw new ArgumentException("Binding keys must be unique. No duplicates allowed.");
            }
        }

        internal override List<string> GetPartitions()
        {
            var partitions = new List<string>();
            partitions.AddRange(BindingKeys.Select(bindingKey => $"{Name}-{bindingKey}"));
            return partitions;
        }

        internal override List<string> GetBindingKeys()
        {
            return BindingKeys.ToList();
        }

        public string[] BindingKeys { get; }
    }
}
