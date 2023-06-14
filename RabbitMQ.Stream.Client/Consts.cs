// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    internal static class Consts
    {
        internal const int MaxBatchSize = 10000;
        internal const int MinBatchSize = 1;
        internal const string RabbitMQClientRepo = "https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/issues";
        internal static readonly TimeSpan ShortWait = TimeSpan.FromSeconds(1);
        internal static readonly TimeSpan MidWait = TimeSpan.FromSeconds(3);
        internal static readonly TimeSpan LongWait = TimeSpan.FromSeconds(10);
        internal const ushort ConsumerInitialCredits = 2;
        internal const byte Version1 = 1;
        internal const byte Version2 = 2;
        internal const string SubscriptionPropertyFilterPrefix = "filter.";
        internal const string SubscriptionPropertyMatchUnfiltered = "match-unfiltered";

        internal static int RandomShort()
        {
            return Random.Shared.Next(500, 1500);
        }

        internal static int RandomMid()
        {
            return Random.Shared.Next(1000, 2500);
        }
    }
}
