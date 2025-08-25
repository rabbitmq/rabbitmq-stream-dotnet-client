// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Net;

namespace RabbitMQ.Stream.Client
{
    public class AddressResolver: IAddressResolver
    {
        public AddressResolver(EndPoint endPoint)
        {
            EndPoint = endPoint;
            Enabled = true;
        }

        public EndPoint EndPoint { get; set; }
        public bool Enabled { get; set; }
        public EndPoint Resolve(string address, int host) => EndPoint;
    }
}
