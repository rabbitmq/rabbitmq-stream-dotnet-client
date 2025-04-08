// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public static class TestExtensions
    {
        public static ReadOnlySequence<byte> AsReadonlySequence(this string s)
        {
            return new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(s));
        }
    }

    public class SystemTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public SystemTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task CreateSystem()
        {
            var config = new StreamSystemConfig
            {
                Endpoints = new List<EndPoint>
                {
                    new IPEndPoint(IPAddress.Loopback, 9999), // this should not be bound
                    new IPEndPoint(IPAddress.Loopback, 5552),
                }
            };
            var system = await StreamSystem.Create(config);
            Assert.False(system.IsClosed);
            await system.Close();
            Assert.True(system.IsClosed);
        }

        [Fact]
        public async Task CreateSystemThrowsWhenNoEndpointsAreReachable()
        {
            var config = new StreamSystemConfig
            {
                Endpoints = new List<EndPoint>
                {
                    new IPEndPoint(IPAddress.Loopback, 9999) // this should not be bound
                }
            };
            await Assert.ThrowsAsync<StreamSystemInitialisationException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async Task CreateSslExceptionThrowsWhenEndPointIsNotSsl()
        {
            // Try to connect to an NoTLS port to using TLS parameters
            var config = new StreamSystemConfig
            {
                Ssl = new SslOption()
                {
                    Enabled = true,
                    AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNotAvailable |
                                             SslPolicyErrors.RemoteCertificateChainErrors |
                                             SslPolicyErrors.RemoteCertificateNameMismatch
                },
            };
            await Assert.ThrowsAsync<SslException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async Task Create_Delete_Stream()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            var spec = new StreamSpec(stream)
            {
                MaxAge = TimeSpan.FromHours(8),
                LeaderLocator = LeaderLocator.Random,
                MaxLengthBytes = 20_000,
                MaxSegmentSizeBytes = 1000
            };
            Assert.Equal("28800s", spec.Args["max-age"]);
            Assert.Equal("random", spec.Args["queue-leader-locator"]);
            Assert.Equal("1000", spec.Args["stream-max-segment-size-bytes"]);
            Assert.Equal("20000", spec.Args["max-length-bytes"]);
            await system.CreateStream(spec);
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async Task Create_Stream_With_Max_Length_Maximum_Value()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            var spec = new StreamSpec(stream) { MaxLengthBytes = ulong.MaxValue };
            await system.CreateStream(spec);
            Assert.Equal(ulong.MaxValue.ToString(), spec.Args["max-length-bytes"]);
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async Task StreamStatus()
        {
            SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
            var stats = await system.StreamStats(stream);

            Assert.Throws<OffsetNotFoundException>(() => { stats.FirstOffset(); }
            );

            Assert.Throws<OffsetNotFoundException>(() => { stats.CommittedChunkId(); }
            );

            await SystemUtils.PublishMessages(system, stream, 500, _testOutputHelper);
            await SystemUtils.WaitAsync();
            var statAfter = await system.StreamStats(stream);
            Assert.Equal((ulong)0, statAfter.FirstOffset());
            Assert.True(statAfter.CommittedChunkId() > 0);
            await SystemUtils.CleanUpStreamSystem(system, stream);
        }

        [Fact]
        public async Task CreateSystemThrowsWhenVirtualHostFailureAccess()
        {
            var config = new StreamSystemConfig { VirtualHost = "DOES_NOT_EXIST" };
            await Assert.ThrowsAsync<VirtualHostAccessFailureException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async Task CreateSystemThrowsWhenAuthenticationAccess()
        {
            var config = new StreamSystemConfig { UserName = "user_does_not_exist" };
            await Assert.ThrowsAsync<AuthenticationFailureException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async Task UpdateSecretWithValidSecretShouldNoRaiseExceptions()
        {
            var config = new StreamSystemConfig { UserName = "guest", Password = "guest" }; // specified for readability
            var streamSystem = await StreamSystem.Create(config);

            await streamSystem.UpdateSecret("guest");
            await streamSystem.Close();
        }

        [Fact]
        public async Task UpdateSecretWithInvalidSecretShouldThrowAuthenticationFailureException()
        {
            var config = new StreamSystemConfig { UserName = "guest", Password = "guest" }; // specified for readability
            var streamSystem = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<AuthenticationFailureException>(
                async () => { await streamSystem.UpdateSecret("not_valid_secret"); }
            );
            await streamSystem.Close();
        }

        [Fact]
        public async Task CreateExistStreamIdempotentShouldNoRaiseExceptions()
        {
            // Create the stream in idempotent way
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            var spec = new StreamSpec(stream);
            // Stream does not exist
            Assert.False(await system.StreamExists(stream));
            await system.CreateStream(spec);
            // Stream just created
            Assert.True(await system.StreamExists(stream));
            await system.CreateStream(spec);
            Assert.True(await system.StreamExists(stream));
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async Task CreateExistStreamPreconditionFailShouldRaiseExceptions()
        {
            // Create the stream in idempotent way
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream) { MaxLengthBytes = 20, });

            await Assert.ThrowsAsync<CreateStreamException>(
                async () => { await system.CreateStream(new StreamSpec(stream) { MaxLengthBytes = 10000, }); }
            );
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async Task ValidateQueryOffset()
        {
            // here we just validate the Query for Offset, Sequence 
            // and Partitions
            // if the reference is == "" return must be 0
            // stream name is mandatory
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.QueryOffset(string.Empty, "stream_we_don_t_care"); }
            );

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.QueryOffset("reference_we_don_care", string.Empty); }
            );

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.QueryOffset(string.Empty, string.Empty); }
            );

            await Assert.ThrowsAsync<QueryException>(
                async () => { await system.QueryPartition("stream_does_not_exist"); }
            );

            await system.Close();
        }

        [Fact]
        public async Task ValidateQuerySequence()
        {
            // here we just validate the Query for Offset and Sequence
            // if the reference is == "" return must be 0
            // stream name is mandatory
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.QuerySequence(string.Empty, "stream_we_don_t_care"); }
            );

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.QuerySequence("reference_we_don_care", string.Empty); }
            );

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.QuerySequence(string.Empty, string.Empty); }
            );
            await system.Close();
        }

        [Fact]
        public async Task ValidateSalsExternalConfiguration()
        {
            // the user can set the SALs configuration externally
            // this test validates that the configuration is supported by the server
            var config = new StreamSystemConfig() { AuthMechanism = AuthMechanism.External };
            await Assert.ThrowsAsync<AuthMechanismNotSupportedException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async Task ValidateRpCtimeOut()
        {
            var config = new StreamSystemConfig() { RpcTimeOut = TimeSpan.FromMilliseconds(1) };
            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async Task CloseProducerConsumerAfterForceCloseShouldNotRaiseError()
        {
            // This tests that the producers and consumers
            // don't raise an exception if the connection is closed
            // by a network problem or forced by the management UI
            // see issues/97
            var stream = Guid.NewGuid().ToString();
            var clientProvidedName = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer =
                await system.CreateRawProducer(
                    new RawProducerConfig(stream) { ClientProvidedName = clientProvidedName });
            await SystemUtils.WaitAsync();
            var consumer = await system.CreateRawConsumer(
                new RawConsumerConfig(stream) { ClientProvidedName = clientProvidedName });
            await SystemUtils.WaitAsync();

            // Here we have to wait the management stats refresh time before killing the connections.
            await SystemUtils.WaitAsync(TimeSpan.FromSeconds(6));

            // we kill _only_ producer and consumer connection
            // leave the locator up and running to delete the stream
            Assert.Equal(2, await SystemUtils.HttpKillConnections(clientProvidedName));
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            // close two time it should not raise an exception
            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            Assert.Equal(ResponseCode.Ok, await consumer.Close());

            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async Task SetHeartBeat()
        {
            // Just test the heartbeat setting
            // TODO find a smarter way to test the heartbeat disconnection
            var config = new StreamSystemConfig() { Heartbeat = TimeSpan.FromMinutes(1), };
            var system = await StreamSystem.Create(config);
            var stream = Guid.NewGuid().ToString();
            await system.CreateStream(new StreamSpec(stream));
            var producer =
                await system.CreateRawProducer(new RawProducerConfig(stream));
            await SystemUtils.WaitAsync();
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async Task NumberOfPartitionsShouldBeAsDefinition()
        {
            await SystemUtils.ResetSuperStreams();
            var system = await StreamSystem.Create(new StreamSystemConfig());
            var partitions = await system.QueryPartition(SystemUtils.InvoicesExchange);
            Assert.True(partitions.Length == 3);
            Assert.Contains(SystemUtils.InvoicesStream0, partitions);
            Assert.Contains(SystemUtils.InvoicesStream1, partitions);
            Assert.Contains(SystemUtils.InvoicesStream2, partitions);
            Assert.DoesNotContain(SystemUtils.InvoicesExchange, partitions);
            await system.Close();
        }

        [Fact]
        public async Task CreateDeleteSuperStream()
        {
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            const string SuperStream = "my-first-system-super-stream";
            var spec = new PartitionsSuperStreamSpec(SuperStream, 2)
            {
                MaxAge = TimeSpan.FromHours(8),
                LeaderLocator = LeaderLocator.Random,
                MaxLengthBytes = 20_000,
                MaxSegmentSizeBytes = 1000,
            };
            Assert.Equal("28800s", spec.Args["max-age"]);
            Assert.Equal("random", spec.Args["queue-leader-locator"]);
            Assert.Equal("1000", spec.Args["stream-max-segment-size-bytes"]);
            Assert.Equal("20000", spec.Args["max-length-bytes"]);
            await system.CreateSuperStream(spec);
            Assert.True(await system.SuperStreamExists(SuperStream));
            await system.DeleteSuperStream(SuperStream);
            Assert.False(await system.SuperStreamExists(SuperStream));
            await system.Close();
        }

        [Fact]
        public async Task ValidateSuperStreamConfiguration()
        {
            const string SuperStream = "my-validation-system-super-stream";
            var system = await StreamSystem.Create(new StreamSystemConfig());
            var specZeroPartitions = new PartitionsSuperStreamSpec(SuperStream, 0) { };

            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.CreateSuperStream(specZeroPartitions); }
            );

            var specNullBindings = new BindingsSuperStreamSpec(SuperStream, null);
            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.CreateSuperStream(specNullBindings); }
            );

            var specDuplicationBindings =
                new BindingsSuperStreamSpec(SuperStream, new[] { "duplication", "duplication" });
            await Assert.ThrowsAsync<ArgumentException>(
                async () => { await system.CreateSuperStream(specDuplicationBindings); }
            );

            await Assert.ThrowsAsync<DeleteStreamException>(
                async () => { await system.DeleteSuperStream("not-exist"); }
            );
            await system.Close();
        }

        [Fact]
        public async Task ClientShouldStoreOffset()
        {
            var stream = Guid.NewGuid().ToString();
            var consumerRef = "myRef";
            var system = await StreamSystem.Create(new StreamSystemConfig());
            await system.CreateStream(new StreamSpec(stream));
            await system.StoreOffset(consumerRef, stream, 4);
            Assert.Equal((ulong)4, await system.QueryOffset(consumerRef, stream));
            await system.DeleteStream(stream);
            await system.Close();
        }
    }
}
