using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    /// <summary>
    /// Routes the client connection to the correct node.
    /// It lookups the leader node for a producer. If case AddressResolver is enabled it tries
    /// until the connection properties Host/Port and  advertisedHost and advertisedPort match.
    /// It lookups a random node (from leader and replicas) for a consumer,
    /// 
    /// </summary>
    internal abstract class RoutingClient
    {
    }

    internal static class RoutingClientHelper
    {
        private static async Task<Client> LookupConnection(ClientParameters clientParameters,
            Broker broker)
        {
            var hostEntry = await Dns.GetHostEntryAsync(broker.Host);
            var leaderEndPoint = new IPEndPoint(hostEntry.AddressList.First(), (int) broker.Port);

            if (clientParameters.AddressResolver == null
                || clientParameters.AddressResolver.Enabled == false)
            {
                // In this case we just return the leader node info since there is not
                // load balancer configuration
                return await Client.Create(clientParameters with {Endpoint = leaderEndPoint});
            }

            // here it means that there is a AddressResolver configuration
            // so there is a load-balancer or proxy we need to get the right connection
            // as first we try with the first node given from the LB
            leaderEndPoint = clientParameters.AddressResolver.EndPoint;
            var client = await Client.Create(clientParameters with {Endpoint = leaderEndPoint});

            string GetPropertyValue(IDictionary<string, string> connectionProperties, string key)
            {
                if (!connectionProperties.TryGetValue(key, out var value))
                {
                    throw new RoutingClientException($"can't lookup {key}");
                }

                return value;
            }

            var advertisedHost = GetPropertyValue(client.ConnectionProperties, "advertised_host");
            var advertisedPort = GetPropertyValue(client.ConnectionProperties, "advertised_port");

            while (broker.Host != advertisedHost ||
                   broker.Port != uint.Parse(advertisedPort))
            {
                await client.Close("advertised_host or advertised_port doesn't mach");

                client = await Client.Create(clientParameters with {Endpoint = leaderEndPoint});
                advertisedHost = GetPropertyValue(client.ConnectionProperties, "advertised_host");
                advertisedPort = GetPropertyValue(client.ConnectionProperties, "advertised_port");

                Thread.Sleep(500);
            }

            return client;
        }

        /// <summary>
        /// Gets the leader connection. The producer must connect to the leader. 
        /// </summary>
        public static async Task<Client> LookupLeaderConnection(ClientParameters clientParameters,
            StreamInfo metaDataInfo)
        {
            return await LookupConnection(clientParameters, metaDataInfo.Leader);
        }
       
        /// <summary>
        /// Gets the Replica connection. The consumer can connect to a replica.
        /// If there are not replicas it can use the Leader. 
        /// </summary>
        public static async Task<Client> LookupReplicaConnection(ClientParameters clientParameters,
            StreamInfo metaDataInfo)
        {
            // if there are no replicas, we have to use the leader 
            if (metaDataInfo.Replicas.Count <= 0) return await LookupConnection(clientParameters, metaDataInfo.Leader);

            // if there are replicas, we can pick one of the node
            // we need to use the LookupConnection since not all the nodes can have a 
            // replicas. So in case of AddressResolver is configured the client could be
            // connected to a node without replicas
            var rnd = new Random();
            var replicaId = rnd.Next(0, metaDataInfo.Replicas.Count);
            var replicaBroker = metaDataInfo.Replicas[replicaId];
            return await LookupConnection(clientParameters, replicaBroker);
        }
    }


    [Serializable]
    public class RoutingClientException : Exception
    {
        public RoutingClientException(string message) : base(message)
        {
        }


        protected RoutingClientException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}