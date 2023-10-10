// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

namespace RabbitMQ.Stream.Client;

public class Info
{
    public string Reference { get; }
    public string Stream { get; }

    internal Info(string reference, string stream)
    {
        Reference = reference;
        Stream = stream;
    }
}
