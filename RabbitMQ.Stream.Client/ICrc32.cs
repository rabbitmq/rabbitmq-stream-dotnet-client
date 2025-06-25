// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.Stream.Client
{
    public enum ChunkAction
    {
        /// <summary>
        /// The consumer will try to process the Chunk.
        /// </summary>
        TryToProcess,

        /// <summary>
        /// The consumer will skip the Chunk and continue processing the next Chunk.
        /// All the messages in the Chunk will be skipped.
        /// </summary>
        Skip
    }

    /// <summary>
    /// ICrc32 defines an interface for implementing crc32 hashing.
    /// Library users who wish to perform crc32 checks on data from RabbitMQ
    /// should implement this interface and assign an instance to
    /// <see cref="IConsumerConfig.Crc32"><code>IConsumerConfig.Crc32</code></see>.
    /// </summary>
    public interface ICrc32
    {
        byte[] Hash(byte[] data);

        /// <summary>
        /// FailAction is called when the Crc32 check fails.
        /// The user can assign a function that returns a <see cref="ChunkAction"/>.
        /// It is possible to add custom logic to handle the failure, such as logging.
        /// The code here should be safe 
        /// </summary>
        Func<IConsumer, ChunkAction> FailAction { get; set; }
    }
}
