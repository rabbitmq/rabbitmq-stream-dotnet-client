// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.Stream.Client;

/// <summary>
/// Default implementation of <see cref="ICrc32"/> using the System.IO.Hashing.Crc32 class.
/// The consumer uses this implementation by default to perform CRC32 checks on chunk received from the server.
/// FailAction is set to null by default, meaning that no custom action is taken when the CRC32 check fails.
/// It uses the default action of skipping the chunk.
/// </summary>
public class StreamCrc32 : ICrc32
{
    public byte[] Hash(byte[] data)
    {
        return System.IO.Hashing.Crc32.Hash(data);
    }

    public Func<IConsumer, ChunkAction> FailAction { get; init; } = null;
    public Func<IConsumer, ChunkInfo, ChunkAction> AsyncFailAction { get; init; }
}
