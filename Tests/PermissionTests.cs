using System;
using System.IO;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class PermissionTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public PermissionTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }


        [Fact]
        [WaitTestBeforeAfter]
        public async void AccessToStreamWithoutGrantsShouldRaiseErrorTest()
        {
            SystemUtils.PostDefinition(SystemUtils.GetFileContent("definition_test.json"));
            // load definition creates users and streams to test the access
            // the user "test" can't access on "no_access_stream"
            const string stream = "no_access_stream";
            var config = new StreamSystemConfig()
            {
                Password = "test",
                UserName = "test",
                VirtualHost = "/"
            };
            var system = await StreamSystem.Create(config);

            await Assert.ThrowsAsync<CreateProducerException>(
                async () =>
                {
                    await system.CreateProducer(
                        new ProducerConfig
                        {
                            Reference = "producer",
                            Stream = stream,
                        });
                }
            );

            await Assert.ThrowsAsync<CreateConsumerException>(
                async () =>
                {
                    await system.CreateConsumer(
                        new ConsumerConfig()
                        {
                            Reference = "consumer",
                            Stream = stream,
                        });
                }
            );

            
            
            await system.Close();
        }
    }
}