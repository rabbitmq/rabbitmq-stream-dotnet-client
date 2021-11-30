using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
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
        public async void CreateStream()
        {
            var stream = Guid.NewGuid().ToString();
            var config = new StreamSystemConfig();
            var system = await StreamSystem.Create(config);
            var spec = new StreamSpec(stream)
            {
                MaxAge = TimeSpan.FromHours(8),
                LeaderLocator = LeaderLocator.Random
            };
            Assert.Equal("28800s", spec.Args["max-age"]);
            Assert.Equal("random", spec.Args["queue-leader-locator"]);
            await system.CreateStream(spec);
            await system.Close();
        }
        
        [Fact]
        public async void CreateSystemThrowsWhenVirtualHostFailureAccess()
        {
            var config = new StreamSystemConfig
            {
                VirtualHost = "DOES_NOT_EXIST"
            };
            await Assert.ThrowsAsync<VirtualHostAccessFailureException>(
                async () => {  await StreamSystem.Create(config); }
            );
        }
    }
}