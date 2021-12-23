using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public abstract class RoutingBase
    {
        public abstract AbstractClient CreateClient(ClientParameters clientParameters);
        public bool ValidateDns { get; set; } = false;
    }
    
    public class Routing : RoutingBase
    {
        public override AbstractClient CreateClient(ClientParameters clientParameters)
        {
            ValidateDns = true;
            var taskClient = Client.Create(clientParameters);
            taskClient.Wait(1000);
            return taskClient.Result;;
        }
    }


    /// <summary>
    /// Routes the client connection to the correct node.
    /// It lookups the leader node for a producer. If case AddressResolver is enabled it tries
    /// until the connection properties Host/Port and  advertisedHost and advertisedPort match.
    /// It lookups a random node (from leader and replicas) for a consumer.
    /// 
    /// </summary>
    public static class RoutingHelper<T> where T : RoutingBase, new()
    {
        private static async Task<AbstractClient> LookupConnection(ClientParameters clientParameters,
            Broker broker)
        {

            var routing = new T();
            
            var leaderEndPoint = new IPEndPoint(IPAddress.Loopback, (int) broker.Port);
            
            if (routing.ValidateDns)
            {
                var hostEntry = await Dns.GetHostEntryAsync(broker.Host);
                leaderEndPoint = new IPEndPoint(hostEntry.AddressList.First(), (int) broker.Port);
            }

            if (clientParameters.AddressResolver == null
                || clientParameters.AddressResolver.Enabled == false)
            {
                // In this case we just return the leader node info since there is not
                // load balancer configuration
                
                return routing.CreateClient(clientParameters with {Endpoint = leaderEndPoint}); ;
            }

            // here it means that there is a AddressResolver configuration
            // so there is a load-balancer or proxy we need to get the right connection
            // as first we try with the first node given from the LB
            leaderEndPoint = clientParameters.AddressResolver.EndPoint;
            var client = routing.CreateClient(clientParameters with {Endpoint = leaderEndPoint});

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

                client = routing.CreateClient(clientParameters with {Endpoint = leaderEndPoint});

                advertisedHost = GetPropertyValue(client.ConnectionProperties, "advertised_host");
                advertisedPort = GetPropertyValue(client.ConnectionProperties, "advertised_port");
                // TODO: Maybe an external parameter
                Thread.Sleep(200);
            }

            return client;
        }

        /// <summary>
        /// Gets the leader connection. The producer must connect to the leader. 
        /// </summary>
        public static async Task<AbstractClient> LookupLeaderConnection(ClientParameters clientParameters,
            StreamInfo metaDataInfo)
        {
            return await LookupConnection(clientParameters, metaDataInfo.Leader);
        }

        /// <summary>
        /// Gets a random connection. The consumer can connect to a replica or leader.
        /// </summary>
        public static async Task<AbstractClient> LookupRandomConnection(ClientParameters clientParameters,
            StreamInfo metaDataInfo)
        {
            var brokers = new List<Broker>() {metaDataInfo.Leader};
            brokers.AddRange(metaDataInfo.Replicas);

            // pick one random node from leader and replicas.
            // we need to use the LookupConnection since not all the nodes can have 
            // replicas. For example if AddressResolver is configured the client could be
            // connected to a node without replicas.
            var rnd = new Random();
            var brokerId = rnd.Next(0, brokers.Count);
            var broker = brokers[brokerId];
            return await LookupConnection(clientParameters, broker);
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