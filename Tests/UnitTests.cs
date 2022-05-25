// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
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
            Parameters = clientParameters;
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

    public class MisconfiguredLoadBalancerRouting : IRouting
    {

        public IClient CreateClient(ClientParameters clientParameters)
        {

            var fake = new FakeClient(clientParameters)
            {
                ConnectionProperties = new Dictionary<string, string>()
                {
                    ["advertised_host"] = "node4",
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
    public class UnitTests
    {
        [Fact]
        public async Task GiveProperExceptionWhenUnableToConnect()
        {
            var clientParameters = new ClientParameters
            {
                // if using `Routing` and not a purpose-built impl of `IRouting`,
                // it's enough to specify an endpoint that won't be reached.
                // Can be replaced with `IRouting` impl that throws on create.
                Endpoint = new DnsEndPoint("localhost", 3939)
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("localhost", 3939), new List<Broker>());
            await Assert.ThrowsAsync<AggregateException>(() => RoutingHelper<Routing>.LookupRandomConnection(clientParameters, metaDataInfo));
        }

        [Fact]
        public async Task AddressResolverShouldRaiseAnExceptionIfAdvIsNull()
        {
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Loopback, 5552));
            var clientParameters = new ClientParameters()
            {
                AddressResolver = addressResolver,
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("leader", 5552), new List<Broker>());
            // run more than one time just to be sure to use all the IP with random
            await Assert.ThrowsAsync<RoutingClientException>(() =>
                RoutingHelper<MissingFieldsRouting>.LookupLeaderConnection(clientParameters, metaDataInfo));
        }

        [Fact]
        public void AddressResolverLoadBalancerSimulate()
        {
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("192.168.10.99"), 5552));
            var clientParameters = new ClientParameters()
            {
                AddressResolver = addressResolver,
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("node2", 5552),
                new List<Broker>() { new Broker("node1", 5552), new Broker("node3", 5552) });
            // run more than one time just to be sure to use all the IP with random
            for (var i = 0; i < 4; i++)
            {
                var client = RoutingHelper<LoadBalancerRouting>.LookupLeaderConnection(clientParameters, metaDataInfo);
                Assert.Equal("node2", client.Result.ConnectionProperties["advertised_host"]);
                Assert.Equal("5552", client.Result.ConnectionProperties["advertised_port"]);
            }
        }

        [Fact]
        public async Task RoutingHelperShouldThrowIfLoadBalancerIsMisconfigured()
        {
            var addressResolver = new AddressResolver(new IPEndPoint(IPAddress.Parse("192.168.10.99"), 5552));
            var clientParameters = new ClientParameters()
            {
                AddressResolver = addressResolver,
            };
            var metaDataInfo = new StreamInfo("stream", ResponseCode.Ok, new Broker("node2", 5552),
                new List<Broker>() { new Broker("replica", 5552) });

            await Assert.ThrowsAsync<RoutingClientException>(
                () => RoutingHelper<MisconfiguredLoadBalancerRouting>.LookupLeaderConnection(clientParameters, metaDataInfo));
        }

        [Fact]
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
            void AssertCompress(List<Message> messages, CompressionType compressionType)
            {
                var codec = CompressionHelper.Compress(messages, compressionType);
                var data = new Span<byte>(new byte[codec.UnCompressedSize]);
                codec.Write(data);
                Assert.True(codec != null);
                Assert.True(data != null);
                var b = new ReadOnlySequence<byte>(data.ToArray());
                var unCompress = CompressionHelper.UnCompress(
                    compressionType,
                    b,
                    (uint)codec.CompressedSize,
                    (uint)codec.UnCompressedSize
                );
                Assert.True(unCompress.Length == codec.UnCompressedSize);
            }

            var messagesTest = new List<Message>();
            PumpMessages(messagesTest);
            AssertCompress(messagesTest, CompressionType.Gzip);
            AssertCompress(messagesTest, CompressionType.None);
        }

        [Fact]
        public void CodeNotFoundException()
        {
            // Raise an exception for the codec not implemented.
            var messages = new List<Message>();

            // codec for CompressionType.Lz4 does not exist.
            Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                CompressionType.Lz4));

            // codec for CompressionType.Snappy does not exist.
            Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                CompressionType.Snappy));

            // codec for CompressionType.Zstd does not exist.
            Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                CompressionType.Zstd));
        }

        private class FakeCodec : ICompressionCodec
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
            public CompressionType CompressionType { get; }

            public ReadOnlySequence<byte> UnCompress(ReadOnlySequence<byte> source, uint dataLen,
                uint unCompressedDataSize)
            {
                throw new NotImplementedException();
            }
        }

        [Fact]
        public void AddRemoveCodecsForType()
        {
            // the following codec aren't provided by builtin.
            // need to register custom codecs
            var types = new List<CompressionType>
            {
                CompressionType.Lz4,
                CompressionType.Snappy,
                CompressionType.Zstd
            };
            foreach (var compressionType in types)
            {
                var messages = new List<Message>();
                // codec for CompressionType does not exist.
                Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                    compressionType));

                // Add codec for CompressionType.
                StreamCompressionCodecs.RegisterCodec<FakeCodec>(compressionType);
                Assert.IsType<FakeCodec>(CompressionHelper.Compress(messages,
                    compressionType));

                StreamCompressionCodecs.UnRegisterCodec(compressionType);

                // codec for CompressionType removed
                Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                    compressionType));
            }
        }

        [Fact]
        public void CodecAlreadyExistException()
        {
            // the following codec aren't provided by builtin.
            // need to register custom codecs
            var types = new List<CompressionType>
            {
                CompressionType.Lz4,
                CompressionType.Snappy,
                CompressionType.Zstd
            };
            foreach (var compressionType in types)
            {
                var messages = new List<Message>();

                // Add codec first time. Ok.
                StreamCompressionCodecs.RegisterCodec<FakeCodec>(compressionType);

                // Exception for the second time
                Assert.Throws<CodecAlreadyExistException>(() =>
                    StreamCompressionCodecs.RegisterCodec<FakeCodec>(compressionType));

                StreamCompressionCodecs.UnRegisterCodec(compressionType);
                // codec for CompressionType removed
                Assert.Throws<CodecNotFoundException>(() => CompressionHelper.Compress(messages,
                    compressionType));
            }
        }
    }
}
