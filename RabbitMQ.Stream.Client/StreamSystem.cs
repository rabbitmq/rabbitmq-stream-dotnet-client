// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client
{
    public record StreamSystemConfig : INamedEntity
    {
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string VirtualHost { get; set; } = "/";
        public TimeSpan Heartbeat { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// TLS options setting.
        /// </summary>
        public SslOption Ssl { get; set; } = new();

        public IList<EndPoint> Endpoints { get; set; } = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) };

        public AddressResolver AddressResolver { get; set; }
        public string ClientProvidedName { get; set; } = "dotnet-stream-locator";

        public AuthMechanism AuthMechanism { get; set; } = AuthMechanism.Plain;

        public byte ConsumersPerConnection { get; set; } = 1;
        public byte ProducersPerConnection { get; set; } = 1;
    }

    public class StreamSystem
    {
        private readonly ClientParameters _clientParameters;
        private Client _client;
        private readonly ILogger<StreamSystem> _logger;
        private ConnectionsPool PoolConsumers { get; init; }
        private ConnectionsPool PoolProducers { get; init; }

        private StreamSystem(ClientParameters clientParameters, Client client,
            byte consumersPerConnection,
            byte producersPerConnection,
            ILogger<StreamSystem> logger = null)
        {
            _clientParameters = clientParameters;
            _client = client;
            _logger = logger ?? NullLogger<StreamSystem>.Instance;
            PoolConsumers = new ConnectionsPool(300, consumersPerConnection);
            PoolProducers = new ConnectionsPool(300, producersPerConnection);
        }

        public bool IsClosed => _client.IsClosed;

        public static async Task<StreamSystem> Create(StreamSystemConfig config, ILogger<StreamSystem> logger = null)
        {
            var clientParams = new ClientParameters
            {
                UserName = config.UserName,
                Password = config.Password,
                VirtualHost = config.VirtualHost,
                Ssl = config.Ssl,
                AddressResolver = config.AddressResolver,
                ClientProvidedName = config.ClientProvidedName,
                Heartbeat = config.Heartbeat,
                Endpoints = config.Endpoints,
                AuthMechanism = config.AuthMechanism
            };
            // create the metadata client connection
            foreach (var endPoint in clientParams.Endpoints)
            {
                try
                {
                    var client = await Client.Create(clientParams with { Endpoint = endPoint }, logger)
                        .ConfigureAwait(false);
                    if (!client.IsClosed)
                    {
                        logger?.LogDebug("Client connected to {@EndPoint}", endPoint);
                        return new StreamSystem(clientParams, client, config.ConsumersPerConnection,
                            config.ProducersPerConnection, logger);
                    }
                }
                catch (Exception e)
                {
                    switch (e)
                    {
                        case ProtocolException or SslException:
                            logger?.LogError(e, "ProtocolException or SslException to {@EndPoint}", endPoint);
                            throw;
                        case AuthMechanismNotSupportedException:
                            logger?.LogError(e, "SalsNotSupportedException to {@EndPoint}", endPoint);
                            throw;
                        default:
                            // hopefully all implementations of endpoint have a nice ToString()
                            logger?.LogError(e, "Error connecting to {@TargetEndpoint}. Trying next endpoint",
                                endPoint);
                            break;
                    }
                }
            }

            throw new StreamSystemInitialisationException("no endpoints could be reached");
        }

        public async Task Close()
        {
            await _client.Close("system close").ConfigureAwait(false);
            _logger?.LogDebug("Client Closed");
        }

        private readonly SemaphoreSlim _semClientProvidedName = new(1);

        private async Task MayBeReconnectLocator()
        {
            var advId = Random.Shared.Next(0, _clientParameters.Endpoints.Count);

            try
            {
                await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
                if (_client.IsClosed)
                {
                    _client = await Client.Create(_client.Parameters with
                    {
                        ClientProvidedName = _clientParameters.ClientProvidedName,
                        Endpoint = _clientParameters.Endpoints[advId]
                    }).ConfigureAwait(false);
                    _logger?.LogDebug("Locator reconnected to {@EndPoint}", _clientParameters.Endpoints[advId]);
                }
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        private static void CheckLeader(StreamInfo metaStreamInfo)
        {
            if (metaStreamInfo.Leader.Equals(default(Broker)))
            {
                throw new LeaderNotFoundException(
                    $"No leader found for streams {string.Join(" ", metaStreamInfo.Stream)}");
            }
        }

        public async Task<IProducer> CreateRawSuperStreamProducer(
            RawSuperStreamProducerConfig rawSuperStreamProducerConfig, ILogger logger = null)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(rawSuperStreamProducerConfig.SuperStream))
            {
                throw new CreateProducerException("Super Stream name can't be empty");
            }

            if (rawSuperStreamProducerConfig.MessagesBufferSize < Consts.MinBatchSize)
            {
                throw new CreateProducerException("Batch Size must be bigger than 0");
            }

            if (rawSuperStreamProducerConfig.Routing == null)
            {
                throw new CreateProducerException("Routing Key Extractor must be provided");
            }

            rawSuperStreamProducerConfig.Client = _client;
            rawSuperStreamProducerConfig.Pool = PoolProducers;

            var partitions = await _client.QueryPartition(rawSuperStreamProducerConfig.SuperStream)
                .ConfigureAwait(false);
            if (partitions.ResponseCode != ResponseCode.Ok)
            {
                throw new CreateProducerException($"producer could not be created code: {partitions.ResponseCode}");
            }

            IDictionary<string, StreamInfo> streamInfos = new Dictionary<string, StreamInfo>();
            foreach (var partitionsStream in partitions.Streams)
            {
                streamInfos[partitionsStream] = await StreamInfo(partitionsStream).ConfigureAwait(false);
            }

            var r = RawSuperStreamProducer.Create(rawSuperStreamProducerConfig,
                streamInfos,
                _clientParameters with { ClientProvidedName = rawSuperStreamProducerConfig.ClientProvidedName },
                logger);
            _logger?.LogDebug("Raw Producer: {ProducerReference} created for SuperStream: {SuperStream}",
                rawSuperStreamProducerConfig.Reference,
                rawSuperStreamProducerConfig.SuperStream);
            return r;
        }

        /// <summary>
        /// Returns the list of partitions for a given super stream
        /// </summary>
        public async Task<string[]> QueryPartition(string superStream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var partitions = await _client.QueryPartition(superStream).ConfigureAwait(false);
            if (partitions.ResponseCode != ResponseCode.Ok)
            {
                throw new QueryException($"query partitions failed code: {partitions.ResponseCode}");
            }

            return partitions.Streams;
        }

        public async Task<IConsumer> CreateSuperStreamConsumer(
            RawSuperStreamConsumerConfig rawSuperStreamConsumerConfig,
            ILogger logger = null)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(rawSuperStreamConsumerConfig.SuperStream))
            {
                throw new CreateProducerException("Super Stream name can't be empty");
            }

            rawSuperStreamConsumerConfig.Client = _client;
            rawSuperStreamConsumerConfig.Pool = PoolConsumers;

            var partitions = await _client.QueryPartition(rawSuperStreamConsumerConfig.SuperStream)
                .ConfigureAwait(false);
            if (partitions.ResponseCode != ResponseCode.Ok)
            {
                throw new CreateConsumerException($"consumer could not be created code: {partitions.ResponseCode}");
            }

            IDictionary<string, StreamInfo> streamInfos = new Dictionary<string, StreamInfo>();
            foreach (var partitionsStream in partitions.Streams)
            {
                streamInfos[partitionsStream] = await StreamInfo(partitionsStream).ConfigureAwait(false);
            }

            var s = RawSuperStreamConsumer.Create(rawSuperStreamConsumerConfig,
                streamInfos,
                _clientParameters with { ClientProvidedName = rawSuperStreamConsumerConfig.ClientProvidedName },
                logger);
            _logger?.LogDebug("Consumer: {Reference} created for SuperStream: {SuperStream}",
                rawSuperStreamConsumerConfig.Reference, rawSuperStreamConsumerConfig.SuperStream);

            return s;
        }

        public async Task<IProducer> CreateRawProducer(RawProducerConfig rawProducerConfig,
            ILogger logger = null)
        {
            if (rawProducerConfig.MessagesBufferSize < Consts.MinBatchSize)
            {
                throw new CreateProducerException("Batch Size must be bigger than 0");
            }

            var metaStreamInfo = await StreamInfo(rawProducerConfig.Stream).ConfigureAwait(false);
            if (metaStreamInfo.ResponseCode != ResponseCode.Ok)
            {
                throw new CreateProducerException($"producer could not be created code: {metaStreamInfo.ResponseCode}");
            }

            CheckLeader(metaStreamInfo);

            try
            {
                await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
                rawProducerConfig.Pool = PoolProducers;
                var p = await RawProducer.Create(
                    _clientParameters with { ClientProvidedName = rawProducerConfig.ClientProvidedName },
                    rawProducerConfig, metaStreamInfo, logger).ConfigureAwait(false);
                _logger?.LogDebug("Raw Producer: {Reference} created for Stream: {Stream}",
                    rawProducerConfig.Reference, rawProducerConfig.Stream);

                return p;
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        private async Task<StreamInfo> StreamInfo(string streamName)
        {
            // force localhost connection for single node clusters and when address resolver is not provided
            // when theres 1 endpoint and an address resolver, there could be a cluster behind a load balancer
            var forceLocalHost = false;
            var localPort = 0;
            if (_clientParameters.Endpoints.Count == 1 &&
                _clientParameters.AddressResolver is null)
            {
                var clientParametersEndpoint = _clientParameters.Endpoints[0];
                switch (clientParametersEndpoint)
                {
                    case DnsEndPoint { Host: "localhost" } dnsEndPoint:
                        forceLocalHost = true;
                        localPort = dnsEndPoint.Port;
                        break;
                    case IPEndPoint ipEndPoint when Equals(ipEndPoint.Address, IPAddress.Loopback):
                        forceLocalHost = true;
                        localPort = ipEndPoint.Port;
                        break;
                }
            }

            StreamInfo metaStreamInfo;
            if (forceLocalHost)
            {
                // craft the metadata response to force using localhost
                var leader = new Broker("localhost", (uint)localPort);
                metaStreamInfo = new StreamInfo(streamName, ResponseCode.Ok, leader,
                    new List<Broker>(1) { leader });
            }
            else
            {
                await MayBeReconnectLocator().ConfigureAwait(false);
                var meta = await _client.QueryMetadata(new[] { streamName }).ConfigureAwait(false);
                metaStreamInfo = meta.StreamInfos[streamName];
            }

            return metaStreamInfo;
        }

        public async Task CreateStream(StreamSpec spec)
        {
            var response = await _client.CreateStream(spec.Name, spec.Args).ConfigureAwait(false);
            if (response.ResponseCode is ResponseCode.Ok or ResponseCode.StreamAlreadyExists)
            {
                return;
            }

            throw new CreateStreamException($"Failed to create stream, error code: {response.ResponseCode.ToString()}");
        }

        public async Task<bool> StreamExists(string stream)
        {
            return await _client.StreamExists(stream).ConfigureAwait(false);
        }

        private static void MaybeThrowQueryException(string reference, string stream)
        {
            if (string.IsNullOrWhiteSpace(reference) || string.IsNullOrWhiteSpace(stream))
            {
                throw new ArgumentException("Stream name and reference can't be empty or null");
            }
        }

        /// <summary>
        /// QueryOffset retrieves the last consumer offset stored
        /// given a consumer name and stream name 
        /// </summary>
        /// <param name="reference">Consumer name</param>
        /// <param name="stream">Stream name</param>
        /// <returns></returns>
        public async Task<ulong> QueryOffset(string reference, string stream)
        {
            MaybeThrowQueryException(reference, stream);

            var response = await _client.QueryOffset(reference, stream).ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"QueryOffset stream: {stream}, reference: {reference}");
            return response.Offset;
        }

        /// <summary>
        /// QuerySequence retrieves the last publishing ID
        /// given a producer name and stream 
        /// </summary>
        /// <param name="reference">Producer name</param>
        /// <param name="stream">Stream name</param>
        /// <returns></returns>
        public async Task<ulong> QuerySequence(string reference, string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            MaybeThrowQueryException(reference, stream);
            var response = await _client.QueryPublisherSequence(reference, stream).ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"QuerySequence stream: {stream}, reference: {reference}");
            return response.Sequence;
        }

        public async Task DeleteStream(string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var response = await _client.DeleteStream(stream).ConfigureAwait(false);
            if (response.ResponseCode == ResponseCode.Ok)
            {
                return;
            }

            throw new DeleteStreamException($"Failed to delete stream, error code: {response.ResponseCode.ToString()}");
        }

        public async Task<StreamStats> StreamStats(string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var response = await _client.StreamStats(stream).ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"StreamStats stream: {stream}");
            return new StreamStats(response.Statistic, stream);
        }

        public async Task<IConsumer> CreateRawConsumer(RawConsumerConfig rawConsumerConfig,
            ILogger logger = null)
        {
            var metaStreamInfo = await StreamInfo(rawConsumerConfig.Stream).ConfigureAwait(false);
            if (metaStreamInfo.ResponseCode != ResponseCode.Ok)
            {
                throw new CreateConsumerException($"consumer could not be created code: {metaStreamInfo.ResponseCode}");
            }

            CheckLeader(metaStreamInfo);

            try
            {
                await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
                rawConsumerConfig.Pool = PoolConsumers;
                var s = _clientParameters with { ClientProvidedName = rawConsumerConfig.ClientProvidedName };
                var c = await RawConsumer.Create(s,
                    rawConsumerConfig, metaStreamInfo, logger).ConfigureAwait(false);
                _logger?.LogDebug("Raw Consumer: {Reference} created for Stream: {Stream}",
                    rawConsumerConfig.Reference, rawConsumerConfig.Stream);

                return c;
            }
            finally
            {
                _semClientProvidedName.Release();
            }
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
        public CreateStreamException(string s) : base(s)
        {
        }
    }

    public class DeleteStreamException : Exception
    {
        public DeleteStreamException(string s) : base(s)
        {
        }
    }

    public class QueryException : Exception
    {
        public QueryException(string s) : base(s)
        {
        }
    }

    public class CreateProducerException : Exception
    {
        public CreateProducerException(string s) : base(s)
        {
        }
    }

    public readonly struct LeaderLocator
    {
        private readonly string _value;

        private LeaderLocator(string value)
        {
            _value = value;
        }

        public static LeaderLocator ClientLocal => new("client-local");
        public static LeaderLocator Random => new("random");
        public static LeaderLocator LeastLeaders => new("least-leaders");

        public override string ToString()
        {
            return _value;
        }
    }

    public class StreamSystemInitialisationException : Exception
    {
        public StreamSystemInitialisationException(string error) : base(error)
        {
        }
    }
}
