﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client
{
    public interface IRouting
    {
        Task<IClient> CreateClient(ClientParameters clientParameters, ILogger logger = null);
        bool ValidateDns { get; set; }
    }

    public class Routing : IRouting
    {
        public bool ValidateDns { get; set; } = true;

        public async Task<IClient> CreateClient(ClientParameters clientParameters, ILogger logger = null)
        {
            return await Client.Create(clientParameters, logger);
        }
    }

    /// <summary>
    /// Routes the client connection to the correct node.
    /// It lookups the leader node for a producer. If case AddressResolver is enabled it tries
    /// until the connection properties Host/Port and  advertisedHost and advertisedPort match.
    /// It lookups a random node (from leader and replicas) for a consumer.
    /// 
    /// </summary>
    public static class RoutingHelper<T> where T : IRouting, new()
    {
        private static async Task<IClient> LookupConnection(
            ClientParameters clientParameters,
            Broker broker,
            int maxAttempts,
            ILogger logger = null
        )
        {
            var routing = new T();

            if (clientParameters.AddressResolver == null
                || clientParameters.AddressResolver.Enabled == false)
            {
                // We use the localhost ip as default
                // this is mostly to have a default value.

                var endPointNoLb = new IPEndPoint(IPAddress.Loopback, (int)broker.Port);

                // ValidateDns just validate the DNS 
                // it the real world application is always TRUE
                // routing.ValidateDns == false is used just for test
                // it should not change.
                if (routing.ValidateDns)
                {
                    var hostEntry = await Dns.GetHostEntryAsync(broker.Host);
                    endPointNoLb = new IPEndPoint(hostEntry.AddressList.First(), (int)broker.Port);
                }

                // In this case we just return the node (leader for producer, random for consumer)
                // since there is not load balancer configuration

                return await routing.CreateClient(clientParameters with { Endpoint = endPointNoLb }, logger);
            }

            // here it means that there is a AddressResolver configuration
            // so there is a load-balancer or proxy we need to get the right connection
            // as first we try with the first node given from the LB
            var endPoint = clientParameters.AddressResolver.EndPoint;
            var client = await routing.CreateClient(clientParameters with { Endpoint = endPoint }, logger);

            var advertisedHost = GetPropertyValue(client.ConnectionProperties, "advertised_host");
            var advertisedPort = GetPropertyValue(client.ConnectionProperties, "advertised_port");

            var attemptNo = 0;
            while (broker.Host != advertisedHost || broker.Port != uint.Parse(advertisedPort))
            {
                attemptNo++;
                await client.Close("advertised_host or advertised_port doesn't match");

                client = await routing.CreateClient(clientParameters with { Endpoint = endPoint }, logger);

                advertisedHost = GetPropertyValue(client.ConnectionProperties, "advertised_host");
                advertisedPort = GetPropertyValue(client.ConnectionProperties, "advertised_port");
                if (attemptNo > maxAttempts)
                {
                    throw new RoutingClientException(
                        $"Could not find broker ({broker.Host}:{broker.Port}) after {maxAttempts} attempts");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }

            return client;
        }

        private static int MaxAttempts(StreamInfo metaDataInfo)
        {
            // Here we have a reasonable number of retry.
            // based on the stream configuration
            // It will retry to the same node more than one time
            // to be sure that there is not some temp fail
            return (int)Math.Pow(2
                                 +
                                 1 // The leader node
                                 +
                                 metaDataInfo.Replicas.Count, // Replicas
                2); // Pow just to be sure that the LoadBalancer will ping all the nodes
        }

        private static string GetPropertyValue(IDictionary<string, string> connectionProperties, string key)
        {
            if (!connectionProperties.TryGetValue(key, out var value))
            {
                throw new RoutingClientException($"can't lookup {key}");
            }

            return value;
        }

        /// <summary>
        /// Gets the leader connection. The producer must connect to the leader. 
        /// </summary>
        public static async Task<IClient> LookupLeaderConnection(ClientParameters clientParameters,
            StreamInfo metaDataInfo, ILogger logger = null)
        {
            return await LookupConnection(clientParameters, metaDataInfo.Leader, MaxAttempts(metaDataInfo), logger);
        }

        /// <summary>
        /// Gets a random connection. The consumer can connect to a replica or leader.
        /// </summary>
        public static async Task<IClient> LookupRandomConnection(ClientParameters clientParameters,
            StreamInfo metaDataInfo, ILogger logger = null)
        {
            var brokers = new List<Broker>() { metaDataInfo.Leader };
            brokers.AddRange(metaDataInfo.Replicas);
            brokers.Sort((_, _) => Random.Shared.Next(-1, 1));
            var exceptions = new List<Exception>();
            foreach (var broker in brokers)
            {
                try
                {
                    return await LookupConnection(clientParameters, broker, MaxAttempts(metaDataInfo), logger);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            throw new AggregateException("None of the brokers could be reached", exceptions);
        }
    }

    [Serializable]
    public class RoutingClientException : Exception
    {
        public RoutingClientException(string message) : base(message)
        {
        }
    }
}
