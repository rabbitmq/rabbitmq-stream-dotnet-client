// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Net;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public interface IAddressResolver
{
    public bool Enabled { get; }

    [Obsolete("Deprecated. Use ResolveAsync instead.")]
    public EndPoint Resolve(string address, int port);

    public Task<EndPoint> ResolveAsync(string address, int port);
}
