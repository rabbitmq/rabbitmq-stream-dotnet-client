﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

namespace RabbitMQ.Stream.Client.AMQP
{
    public class ApplicationProperties : Map<string>
    {
        public ApplicationProperties()
        {
            MapDataCode = DescribedFormatCode.ApplicationProperties;
        }
    }
}