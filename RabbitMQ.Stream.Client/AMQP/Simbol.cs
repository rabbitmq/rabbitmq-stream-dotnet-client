// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

namespace RabbitMQ.Stream.Client.AMQP;

public class Simbol
{
    public Simbol(string value)
    {
        Value = value;
    }

    public override string ToString()
    {
        return Value;
    }

    public string Value { get; }
}
