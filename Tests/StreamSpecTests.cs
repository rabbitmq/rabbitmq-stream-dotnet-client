using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests
{


    [Collection("Sequential")]
    public class StreamSpecTests
    {
        private readonly ITestOutputHelper testOutputHelper;

        public StreamSpecTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        
        [Fact]
        [WaitTestBeforeAfter]
        public void DefaultStreamSpecMustHaveAtLeastQueueLeaderLocator()
        {
            StreamSpec actualSpec = new StreamSpec("theStreamName");
            StreamSpec expectedSpec = new StreamSpec("theStreamName") {
                LeaderLocator = LeaderLocator.LeastLeaders
            };
            Assert.Equal(expectedSpec.Args, actualSpec.Args);

        }


        [Fact]
        [WaitTestBeforeAfter]
        public void CanOverrideAnyStreamSpecAttributes()
        {
            StreamSpec spec = new StreamSpec("theStreamName");
            spec.MaxAge =  TimeSpan.FromHours(3);
            spec.MaxLengthBytes = 10000;
            spec.LeaderLocator = LeaderLocator.Random; // this is an override because the spec has already a default value

            // can override any settings being set
            spec.MaxAge =  TimeSpan.FromHours(5);
            spec.MaxLengthBytes = 20000;


            StreamSpec expectedSpec = new StreamSpec("theStreamName") {
                LeaderLocator = LeaderLocator.Random,
                MaxLengthBytes = 20000,
                MaxAge = TimeSpan.FromHours(5)
            };   
            Assert.Equal(expectedSpec.Args, spec.Args);
        }

    }
}
