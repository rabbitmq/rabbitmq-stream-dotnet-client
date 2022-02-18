// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Net;

namespace RabbitMQ.Stream.Client
{
    public class AddressResolver
    {
        public AddressResolver(IPEndPoint endPoint)
        {
            EndPoint = endPoint;
            Enabled = true;
        }

        public IPEndPoint EndPoint { get; set; }
        public bool Enabled { get; set; }
    }
}
