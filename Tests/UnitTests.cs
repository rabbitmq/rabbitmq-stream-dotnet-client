using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
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


        [Fact]
        [WaitTestBeforeAfter]
        public void CompressUnCompressShouldHaveTheSize()
        {
            void PumpMessages(ICollection<Message> messages)
            {
                for (var i = 0; i < 54321; i++)
                {
                    messages.Add(new Message(Encoding.UTF8.GetBytes($"data_{i}")));
                }
            }

            // test the compress routine
            // The uncompressed data len _must_ be equal to the original iCompress.UnCompressedSize
            // 
            void AssertCompress(List<Message> messages, CompressionMode compressionMode)
            {
                var codec = CompressionHelper.Compress(messages, compressionMode);
                var data = new Span<byte>(new byte[codec.UnCompressedSize]);
                codec.Write(data);
                Assert.True(codec != null);
                Assert.True(data != null);
                var b = new ReadOnlySequence<byte>(data.ToArray());
                var unCompress = CompressionHelper.UnCompress(
                    compressionMode,
                    b,
                    (uint) codec.CompressedSize,
                    (uint) codec.UnCompressedSize
                );
                Assert.True(unCompress.Length == codec.UnCompressedSize);
            }

            var messagesTest = new List<Message>();
            PumpMessages(messagesTest);
            AssertCompress(messagesTest, CompressionMode.Gzip);
            AssertCompress(messagesTest, CompressionMode.None);
        }


        [Fact]
        [WaitTestBeforeAfter]
        public void CodeNotFoundException()
        {
            // Raise an exception for the codec not implemented.
            var messages = new List<Message>();

            // codec for CompressionMode.Lz4 does not exist.
            Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages, 
                CompressionMode.Lz4));
            
            // codec for CompressionMode.Snappy does not exist.
            Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages, 
                CompressionMode.Snappy));
            
            // codec for CompressionMode.Zstd does not exist.
            Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages, 
                CompressionMode.Zstd));
        }


        private class FakeCodec: ICompressionCodec
        {
            public void Compress(List<Message> messages)
            {
                // nothing to do
            }

            public int Write(Span<byte> span)
            {
                throw new NotImplementedException();
            }

            public int CompressedSize { get; }
            public int UnCompressedSize { get; }
            public int MessagesCount { get; }
            public CompressionMode CompressionMode { get; }
            public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen, uint unCompressedDataSize)
            {
                throw new NotImplementedException();
            }
        }
        [Fact]
        [WaitTestBeforeAfter]
        public void AddNewCodecShouldNotRaiseException()
        {
            // the following codec aren't provided by builtin.
            // need to register custom codecs
            var types = new List<CompressionMode>
            {
                CompressionMode.Lz4,
                CompressionMode.Snappy,
                CompressionMode.Zstd
            };
            foreach (var compressionMode in types)
            {
                var messages = new List<Message>();
                // codec for compressionMode does not exist.
                Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                    compressionMode));

                // Add codec for compressionMode.
                StreamCompressionCodecs.RegisterCodec<FakeCodec>(compressionMode);
                Assert.IsType<FakeCodec>(CompressionHelper.Compress(messages,
                    compressionMode));

                StreamCompressionCodecs.UnRegisterCodec(compressionMode);

                // codec for compressionMode removed
                Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                    compressionMode));
            }

        }
    }
}