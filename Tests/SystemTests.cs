// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Net.Security;
using System.Text;
using RabbitMQ.Stream.Client;
using Xunit;

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
        [Fact]
        public async void CreateSystem()
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
        public async void CreateSystemThrowsWhenNoEndpointsAreReachable()
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
        public async void CreateSslExceptionThrowsWhenEndPointIsNotSsl()
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
        public async void Create_Delete_Stream()
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
        public async void CreateSystemThrowsWhenVirtualHostFailureAccess()
        {
            var config = new StreamSystemConfig { VirtualHost = "DOES_NOT_EXIST" };
            await Assert.ThrowsAsync<VirtualHostAccessFailureException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async void CreateSystemThrowsWhenAuthenticationAccess()
        {
            var config = new StreamSystemConfig { UserName = "user_does_not_exist" };
            await Assert.ThrowsAsync<AuthenticationFailureException>(
                async () => { await StreamSystem.Create(config); }
            );
        }

        [Fact]
        public async void CreateExistStreamIdempotentShouldNoRaiseExceptions()
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
        public async void CreateExistStreamPreconditionFailShouldRaiseExceptions()
        {
            // Create the stream in idempotent way
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream) { MaxLengthBytes = 20, });

            await Assert.ThrowsAsync<CreateStreamException>(
                async () =>
                {
                    await system.CreateStream(new StreamSpec(stream) { MaxLengthBytes = 10000, });
                }
            );
            await system.DeleteStream(stream);
            await system.Close();
        }

        [Fact]
        public async void ValidateQueryOffset()
        {
            // here we just validate the Query for Offset and Sequence
            // if the reference is == "" return must be 0
            // stream name is mandatory
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<QueryException>(
                async () =>
                {
                    await system.QueryOffset(string.Empty, "stream_we_don_t_care");
                }
            );

            await Assert.ThrowsAsync<QueryException>(
                async () =>
                {
                    await system.QueryOffset("reference_we_don_care", string.Empty);
                }
            );

            await Assert.ThrowsAsync<QueryException>(
                async () =>
                {
                    await system.QueryOffset(string.Empty, string.Empty);
                }
            );
            await system.Close();
        }

        [Fact]
        public async void ValidateQuerySequence()
        {
            // here we just validate the Query for Offset and Sequence
            // if the reference is == "" return must be 0
            // stream name is mandatory
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<QueryException>(
                async () =>
                {
                    await system.QuerySequence(string.Empty, "stream_we_don_t_care");
                }
            );

            await Assert.ThrowsAsync<QueryException>(
                async () =>
                {
                    await system.QuerySequence("reference_we_don_care", string.Empty);
                }
            );

            await Assert.ThrowsAsync<QueryException>(
                async () =>
                {
                    await system.QuerySequence(string.Empty, string.Empty);
                }
            );
            await system.Close();
        }

        [Fact]
        public async void CloseProducerConsumerAfterForceCloseShouldNotRaiseError()
        {
            // This tests that the producers and consumers
            // don't raise an exception if the connection is closed
            // by a network problem or forced by the management UI
            // see issues/97
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            await system.CreateStream(new StreamSpec(stream));
            var producer = await system.CreateProducer(new ProducerConfig { Stream = stream, ClientProvidedName = "to_kill" });
            SystemUtils.Wait();
            var consumer = await system.CreateConsumer(
                new ConsumerConfig { Stream = stream, ClientProvidedName = "to_kill" });
            SystemUtils.Wait();

            // Here we have to wait the management stats refresh time before killing the connections.
            SystemUtils.Wait(TimeSpan.FromSeconds(6));

            // we kill _only_ producer and consumer connection
            // leave the locator up and running to delete the stream
            Assert.Equal(2, SystemUtils.HttpKillConnections("to_kill").Result);
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            Assert.Equal(ResponseCode.Ok, await producer.Close());
            // close two time it should not raise an exception
            Assert.Equal(ResponseCode.Ok, await consumer.Close());
            Assert.Equal(ResponseCode.Ok, await consumer.Close());

            await system.DeleteStream(stream);
            await system.Close();
        }
    }
}
