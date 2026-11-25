// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public class DnsAddressResolver : IAddressResolver
    {
        public DnsAddressResolver(DnsEndPoint endPoint)
        {
            EndPoint = endPoint;
            Enabled = true;
        }

        public EndPoint EndPoint { get; set; }
        public bool Enabled { get; set; }
        [Obsolete("Deprecated. Use ResolveAsync instead.")]
        public EndPoint Resolve(string address, int port) => ResolveAsync(address, port).GetAwaiter().GetResult();
        public async Task<EndPoint> ResolveAsync(string address, int port)
        {
            var dnsEndPoint = (DnsEndPoint)EndPoint;
            var entries = await Dns.GetHostEntryAsync(dnsEndPoint.Host).ConfigureAwait(false);
            var addressList = entries.AddressList;
            if (dnsEndPoint.AddressFamily != AddressFamily.Unspecified)
            {
                addressList = Array.FindAll(addressList, ip => ip.AddressFamily == dnsEndPoint.AddressFamily);
            }

            if (addressList.Length == 0)
            {
                throw new SocketException((int)SocketError.NoData);
            }

            var targetIp = addressList[Random.Shared.Next(addressList.Length)];
            return new IPEndPoint(targetIp, port);
        }
    }
}
