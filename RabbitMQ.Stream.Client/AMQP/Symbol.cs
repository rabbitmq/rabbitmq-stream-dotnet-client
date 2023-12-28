// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.Stream.Client.AMQP;

internal class Symbol
{
    private readonly string _value;

    public Symbol(string value)
    {
        _value = value;
    }

    public string Value
    {
        get
        {
            return _value;
        }
    }

    public bool IsNull
    {
        get
        {
            return string.IsNullOrWhiteSpace(_value);
        }
    }

    public override string ToString()
    {
        return Value;
    }
}
