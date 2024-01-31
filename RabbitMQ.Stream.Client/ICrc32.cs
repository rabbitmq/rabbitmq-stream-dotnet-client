﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.Stream.Client
{
    /// <summary>
    /// ICrc32 defines an interface for implementing crc32 hashing.
    /// Library users who wish to perform crc32 checks on data from RabbitMQ
    /// should implement this interface and assign an instance to
    /// <see cref="IConsumerConfig.Crc32"><code>IConsumerConfig.Crc32</code></see>.
    /// </summary>
    public interface ICrc32
    {
        byte[] Hash(byte[] data);
    }
}
