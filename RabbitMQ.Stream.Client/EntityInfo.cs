// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;

namespace RabbitMQ.Stream.Client;

public abstract class Info
{
    public string Identifier { get; }
    public string Stream { get; }
    public List<string> Partitions { get; }

    protected Info(string stream, string identifier, List<string> partitions)
    {
        Stream = stream;
        Identifier = identifier;
        Partitions = partitions;
    }
}
