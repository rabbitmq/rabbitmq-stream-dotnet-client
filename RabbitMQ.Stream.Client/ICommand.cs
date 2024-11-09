// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public interface ICommand
    {
        ushort Version => 1;
        uint CorrelationId => uint.MaxValue;
        int SizeNeeded { get; }
        int Write(IBufferWriter<byte> writer);
    }
}
