// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    /// <summary>
    /// IClient is the Interface for the actual Client
    /// It is needed to create unit tests hard to test using
    /// Integration tests: See AddressResolver tests.
    /// </summary>
    public interface IClient
    {
        public ClientParameters Parameters { get; set; }
        public IDictionary<string, string> ConnectionProperties { get; }

        public Task<CloseResponse> Close(string reason);
    }
}
