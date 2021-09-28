using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public record StreamSystemConfig
    {
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public IList<EndPoint> Endpoints { get; set; } = new List<EndPoint> {new IPEndPoint(IPAddress.Loopback, 5552)};
    }
    
    public class StreamSystem
    {
        private readonly StreamSystemConfig config;
        private readonly ClientParameters clientParameters;
        private readonly Client client;

        private StreamSystem(StreamSystemConfig config, ClientParameters clientParameters, Client client)
        {
            this.config = config;
            this.clientParameters = clientParameters;
            this.client = client;
        }

        public bool IsClosed => client.IsClosed;

        public static async Task<StreamSystem> Create(StreamSystemConfig config)
        {
            var clientParams = new ClientParameters
            {
                UserName = config.UserName,
                Password = config.Password
            };
            // create the metadata client connection
            foreach (var endPoint in config.Endpoints)
            {
                try
                {
                    var client = await Client.Create(clientParams with {Endpoint = endPoint});
                    if (!client.IsClosed)
                        return new StreamSystem(config, clientParams, client);
                }
                catch (Exception)
                {
                   //TODO log? 
                }
                    
            }

            throw new StreamSystemInitialisationException("no endpoints could be reached");
        }

        public async Task Close()
        {
            await client.Close("system close");
        }

        public async Task<Producer> CreateProducer(ProducerConfig producerConfig)
        {
            var meta = await client.QueryMetadata(new []{producerConfig.Stream});
            //TODO: error handling
            var info = meta.StreamInfos[producerConfig.Stream];
            var hostEntry = await Dns.GetHostEntryAsync(info.Leader.Host);
            var ipEndpoint = new IPEndPoint(hostEntry.AddressList.First(), (int)info.Leader.Port);
            // first look up meta data for stream
            //then connect producer to that node
            return await Producer.Create(clientParameters with {Endpoint = ipEndpoint}, producerConfig); 
        }

        public async Task CreateStream(StreamSpec spec)
        {
            var response = await client.CreateStream(spec.Name, spec.Args);
            if (response.ResponseCode == ResponseCode.Ok)
                return;
            throw new CreateStreamException($"Failed to create stream, error code: {response.ResponseCode.ToString()}");
        }

        public async Task<Consumer> CreateConsumer(ConsumerConfig consumerConfig)
        {
            var meta = await client.QueryMetadata(new []{consumerConfig.Stream});
            //TODO: error handling
            var info = meta.StreamInfos[consumerConfig.Stream];
            var hostEntry = await Dns.GetHostEntryAsync(info.Leader.Host);
            var ipEndpoint = new IPEndPoint(hostEntry.AddressList.First(), (int)info.Leader.Port);
            // first look up meta data for stream
            //then connect producer to that node
            return await Consumer.Create(clientParameters with {Endpoint = ipEndpoint}, consumerConfig); 
        }
    }

    public class CreateConsumerException : Exception
    {
        public CreateConsumerException(string s) : base(s)
        {
        }
    }

    public class CreateStreamException : Exception
    {
        public CreateStreamException(string s) : base(s) { }
    }

    public struct Properties
    {
        
    }

    public readonly struct LeaderLocator
    {
        private readonly string value;

        private LeaderLocator(string value)
        {
            this.value = value;
        }

        public static LeaderLocator ClientLocal => new LeaderLocator("client-local");
        public static LeaderLocator Random => new LeaderLocator("random");
        public static LeaderLocator LeastLeaders => new LeaderLocator("least-leaders");

        public override string ToString()
        {
            return value;
        }
    }

    public class StreamSystemInitialisationException : Exception
    {
        public StreamSystemInitialisationException(string error) : base(error)
        {
        }
    }
}