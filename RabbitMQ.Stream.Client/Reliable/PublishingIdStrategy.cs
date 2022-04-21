// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

/// <summary>
/// Define PublishingId Strategy.
/// Can be automatic, so the ReliableProducer will provide
/// the PublishingId
/// </summary>
public interface IPublishingIdStrategy
{
    ulong GetPublishingId();
    Task InitPublishingId();
}
