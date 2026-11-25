// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    public class DnsAddressResolverTests
    {
        [Theory]
        [InlineData(AddressFamily.InterNetwork)]
        [InlineData(AddressFamily.InterNetworkV6)]
        [InlineData(AddressFamily.Unspecified)]
        public async Task ResolveAsyncHonorsAddressFamily(AddressFamily family)
        {
            const string host = "localhost";
            const int port = 5552;
            var addresses = (await Dns.GetHostEntryAsync(host)).AddressList;
            var eligible = addresses.Where(ip => family == AddressFamily.Unspecified || ip.AddressFamily == family).ToArray();
            var resolver = new DnsAddressResolver(new DnsEndPoint(host, port, family));

            if (eligible.Length == 0)
            {
                var exception = await Assert.ThrowsAsync<SocketException>(() => resolver.ResolveAsync(host, port));
                Assert.Equal(SocketError.NoData, exception.SocketErrorCode);
                return;
            }

            // Sample multiple times so a lucky pick on a dual-stack host cannot mask the bug.
            for (var attempt = 0; attempt < 32; attempt++)
            {
                var endpoint = Assert.IsType<IPEndPoint>(await resolver.ResolveAsync(host, port));
                Assert.Contains(endpoint.Address, eligible);
                Assert.Equal(port, endpoint.Port);
            }
        }
    }
}
