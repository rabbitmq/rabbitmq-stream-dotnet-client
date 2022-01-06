using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    public class FakeClient : IClient
    {
        public ClientParameters Parameters { get; set; }
        public IDictionary<string, string> ConnectionProperties { get; init; } 

        public Task<CloseResponse> Close(string reason)
        {
            return Task.FromResult(new CloseResponse());
        }

        public FakeClient(ClientParameters clientParameters)
        {
            this.Parameters = clientParameters;
        }
    }

    public class LoadBalancerRouting : IRouting
    {
        private readonly List<string> advertisedHosts = new()
        {
            "node1",
            "node2",
            "node3",
        };

        // Simulate a load-balancer access using random 
        // access to the advertisedHosts list
        public IClient CreateClient(ClientParameters clientParameters)
        {
            var rnd = new Random();
            var advId = rnd.Next(0, advertisedHosts.Count);

            var fake = new FakeClient(clientParameters)
            {
                ConnectionProperties = new Dictionary<string, string>()
                {
                    ["advertised_host"] = advertisedHosts[advId],
                    ["advertised_port"] = "5552"
                }
            };
            return fake;
        }

        public bool ValidateDns { get; set; } = false;
    }


    //advertised_host is is missed
    public class MissingFieldsRouting : IRouting
    {
        public IClient CreateClient(ClientParameters clientParameters)
        {
            var fake = new FakeClient(clientParameters)
            {
                ConnectionProperties = new Dictionary<string, string>()
                {
                    ["advertised_port"] = "5552"
                }
            };
            return fake;
        }

        public bool ValidateDns { get; set; } = false;
    }


    public class ReplicaRouting : IRouting
    {
        public IClient CreateClient(ClientParameters clientParameters)
        {
            var fake = new FakeClient(clientParameters)
            {
                ConnectionProperties = new Dictionary<string, string>()
                {
                    ["advertised_port"] = "5552",
                    ["advertised_host"] = "leader"
                }
            };
            return fake;
        }

        public bool ValidateDns { get; set; } = false;
    }


    // This class is only for unit tests
    [Collection("Sequential")]
    public class UnitTests
    {
        [Fact]
        [WaitTestBeforeAfter]
        public void AddressResolverShouldRaiseAnExceptionIfAdvIsNull()
        {
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Loopback, 5552));
            var clientParameters = new ClientParameters()
            {
                AddressResolver = addressResolver,
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("leader", 5552),
                null);
            // run more than one time just to be sure to use all the IP with random
            Assert.ThrowsAsync<RoutingClientException>(() =>
                RoutingHelper<MissingFieldsRouting>.LookupLeaderConnection(clientParameters, metaDataInfo));
        }

        [Fact]
        [WaitTestBeforeAfter]
        public void AddressResolverLoadBalancerSimulate()
        {
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("192.168.10.99"), 5552));
            var clientParameters = new ClientParameters()
            {
                AddressResolver = addressResolver,
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("node2", 5552),
                new List<Broker>() {new Broker("replica", 5552)});
            // run more than one time just to be sure to use all the IP with random
            for (var i = 0; i < 4; i++)
            {
                var client = RoutingHelper<LoadBalancerRouting>.LookupLeaderConnection(clientParameters, metaDataInfo);
                Assert.Equal("node2", client.Result.ConnectionProperties["advertised_host"]);
                Assert.Equal("5552", client.Result.ConnectionProperties["advertised_port"]);
            }
        }


        [Fact]
        [WaitTestBeforeAfter]
        public void RandomReplicaLeader()
        {
            // this test is not completed yet should add also some replicas
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("192.168.10.99"), 5552));
            var clientParameters = new ClientParameters()
            {
                AddressResolver = addressResolver,
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("leader", 5552),
                new List<Broker>());
            var client = RoutingHelper<ReplicaRouting>.LookupRandomConnection(clientParameters, metaDataInfo);
            Assert.Equal("5552", client.Result.ConnectionProperties["advertised_port"]);
            var res = (client.Result.ConnectionProperties["advertised_host"] == "leader" ||
                       client.Result.ConnectionProperties["advertised_host"] == "replica");
            Assert.True(res);
        }
    }
}