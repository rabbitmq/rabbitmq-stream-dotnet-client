// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.Stream.Client;

/// <summary>
/// ILookupLocatorStrategy is the interface to implement a strategy to lookup the connection locator.
/// In case of connection failure, the client will try
/// to lookup the connection locator again to get a new endpoint to connect to.
/// </summary>
public interface ILookupLocatorStrategy
{
    int MaxAttempts { get; init; }

    TimeSpan Delay { get; }
}

internal class BackOffLookupLocatorStrategy : ILookupLocatorStrategy
{
    private TimeSpan _delay = TimeSpan.FromMilliseconds(1000);
    public int MaxAttempts { get; init; } = 10;

    public TimeSpan Delay
    {
        get
        {
            _delay = TimeSpan.FromMilliseconds(Random.Shared.Next(10) * 1000) + _delay;
            return _delay;
        }
    }
}
