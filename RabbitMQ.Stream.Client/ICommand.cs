// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    public interface ICommand
    {
        ushort Version => 1;
        uint CorrelationId => uint.MaxValue;
        public int SizeNeeded { get; }
        int Write(Span<byte> span);
    }
}
