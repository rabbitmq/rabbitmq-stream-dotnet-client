// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using RabbitMQ.Stream.Client;

namespace Tests;

public class Crc32 : ICrc32
{
    public byte[] Hash(byte[] data)
    {
        return System.IO.Hashing.Crc32.Hash(data);
    }
}
