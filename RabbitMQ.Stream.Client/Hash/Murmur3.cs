// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Security.Cryptography;

namespace RabbitMQ.Stream.Client.Hash;

public abstract class Murmur3 : HashAlgorithm
{
    protected const uint C1 = 0xcc9e2d51;
    protected const uint C2 = 0x1b873593;

    protected Murmur3(uint seed)
    {
        Seed = seed;
        Reset();
    }

    public override int HashSize => 32;
    public uint Seed { get; }

    protected uint H1 { get; set; }

    protected int Length { get; set; }

    private void Reset()
    {
        H1 = Seed;
        Length = 0;
    }

    public override void Initialize()
    {
        Reset();
    }

    protected override byte[] HashFinal()
    {
        H1 = (H1 ^ (uint)Length).FMix();

        return BitConverter.GetBytes(H1);
    }
}
