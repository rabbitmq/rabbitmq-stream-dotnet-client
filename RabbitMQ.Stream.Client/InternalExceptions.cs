// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.Stream.Client
{
    internal class NotEnoughDataException : Exception
    {
        public NotEnoughDataException(string s)
            : base(s)
        {
        }
    }
}
