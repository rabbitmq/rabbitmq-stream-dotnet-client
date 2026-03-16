// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client
{
    /// <summary>
    /// Configuration for connecting to a RabbitMQ Stream and creating a <see cref="StreamSystem"/>.
    /// </summary>
    public record StreamSystemConfig : INamedEntity
    {
        internal void Validate()
        {
            if (ConnectionPoolConfig is null)
            {
                throw new ArgumentException("ConnectionPoolConfig can't be null");
            }

            if (RpcTimeOut < TimeSpan.FromSeconds(1))
            {
                throw new ArgumentException("RpcTimeOut must be at least 1 second");
            }
        }

        /// <summary>
        /// Username for authentication. Default is "guest".
        /// </summary>
        public string UserName { get; set; } = "guest";

        /// <summary>
        /// Password for authentication. Default is "guest".
        /// </summary>
        public string Password { get; set; } = "guest";

        /// <summary>
        /// Virtual host to use. Default is "/".
        /// </summary>
        public string VirtualHost { get; set; } = "/";

        /// <summary>
        /// Heartbeat interval for the connection. Default is 1 minute.
        /// </summary>
        public TimeSpan Heartbeat { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// TLS options setting.
        /// </summary>
        public SslOption Ssl { get; set; } = new();

        /// <summary>
        /// List of broker endpoints to connect to. The client tries each in order until one succeeds.
        /// Default is localhost on port 5552 (stream protocol port).
        /// </summary>
        public IList<EndPoint> Endpoints { get; set; } =
            new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) };

        /// <summary>
        /// Optional resolver to translate stream leader addresses (e.g. for load balancers or DNS).
        /// </summary>
        public IAddressResolver AddressResolver { get; set; }

        /// <summary>
        /// ClientProvidedName is a string that identifies the client connection.
        /// It is used in the management UI and in the connection details.
        /// </summary>

        public string ClientProvidedName { get; set; } = "dotnet-stream-locator";

        /// <summary>
        /// Authentication mechanism. Default is <see cref="AuthMechanism.Plain"/>.
        /// </summary>
        public AuthMechanism AuthMechanism { get; set; } = AuthMechanism.Plain;

        /// <summary>
        ///  Configure the connection pool for producers and consumers.
        /// See <see cref="ConnectionPoolConfig"/> for more details about the configuration options.
        /// </summary>
        public ConnectionPoolConfig ConnectionPoolConfig { get; set; } = new();

        /// <summary>
        ///  The timeout for RPC calls, like PeerProperties, QueryMetadata, etc.
        /// Default value is 10 seconds and in most cases it should be enough.
        /// Low value can cause false errors in the client.
        /// </summary>
        public TimeSpan RpcTimeOut { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// See <see cref="SocketOptions"/> for configurable TCP socket options for a connection. Use this to tune buffer sizes,
        /// Nagle's algorithm, linger on close, and TCP keep-alive.
        /// </summary>
        public SocketOptions SocketOptions { get; set; } = null;

        /// <summary>
        /// LookupLocatorStrategy see <see cref="ILookupLocatorStrategy"/> for the strategy to reconnect the locator client in case of connection failure.
        /// </summary>
        public ILookupLocatorStrategy LookupLocatorStrategy { get; set; } = new BackOffLookupLocatorStrategy();
    }

    /// <summary>
    /// StreamSystem is the main entry point of the client.
    /// It is responsible for managing the connections to the server and providing methods to create producers and consumers.
    /// Implements IAsyncDisposable to allow using <c>await using</c> and automatic cleanup producers, consumers.
    /// </summary>
    public class StreamSystem : IAsyncDisposable
    {
        private readonly ClientParameters _clientParameters;
        private Client _client;
        private readonly ILogger<StreamSystem> _logger;
        private ConnectionsPool PoolConsumers { get; init; }
        private ConnectionsPool PoolProducers { get; init; }

        private StreamSystem(ClientParameters clientParameters, Client client,
            ConnectionPoolConfig connectionPoolConfig,
            ILogger<StreamSystem> logger = null)
        {
            _clientParameters = clientParameters;
            _client = client;
            _logger = logger ?? NullLogger<StreamSystem>.Instance;
            // we don't expose the max connections per producer/consumer
            // for the moment. We can expose it in the future if needed
            PoolConsumers = new ConnectionsPool(0,
                connectionPoolConfig.ConsumersPerConnection, connectionPoolConfig.ConnectionCloseConfig);

            PoolProducers = new ConnectionsPool(0,
                connectionPoolConfig.ProducersPerConnection, connectionPoolConfig.ConnectionCloseConfig);
        }

        /// <summary>
        /// Indicates whether the stream system's locator connection is closed.
        /// </summary>
        public bool IsClosed => _client.IsClosed;

        /// <summary>
        /// Creates and connects a <see cref="StreamSystem"/> using the given configuration.
        /// Tries each endpoint in <see cref="StreamSystemConfig.Endpoints"/> until one succeeds.
        /// </summary>
        /// <param name="config">Connection and pool configuration.</param>
        /// <param name="logger">Optional logger for connection and lifecycle events.</param>
        /// <returns>A connected <see cref="StreamSystem"/> instance.</returns>
        /// <exception cref="StreamSystemInitialisationException">Thrown when no endpoint could be reached.</exception>
        public static async Task<StreamSystem> Create(StreamSystemConfig config, ILogger<StreamSystem> logger = null)
        {
            config.Validate();

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
                AuthMechanism = config.AuthMechanism,
                RpcTimeOut = config.RpcTimeOut,
                SocketOptions = config.SocketOptions,
                LookupLocatorStrategy = config.LookupLocatorStrategy,
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
                        return new StreamSystem(clientParams, client, config.ConnectionPoolConfig,
                            logger);
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

        /// <summary>
        /// Closes the stream system: closes the locator connection and all producer/consumer connection pools.
        /// </summary>
        public async Task Close()
        {
            await _client.Close("system close").ConfigureAwait(false);

            try
            {
                await PoolConsumers.Close()
                    .ConfigureAwait(false);
                await PoolProducers.Close()
                    .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger?.LogError(e, "Error closing stream system");
            }
            finally
            {
                PoolConsumers.Dispose();
                PoolProducers.Dispose();
            }

            _logger?.LogDebug("Stream system closed");
        }

        /// <summary>
        /// Disposes the stream system asynchronously, closing all connections and releasing resources.
        /// Enables <c>await using</c> and automatic cleanup by dependency injection containers.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await Close().ConfigureAwait(false);

            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        private readonly SemaphoreSlim _semClientProvidedName = new(1);

        /// <summary>
        /// MayBeReconnectLocator tries to reconnect the locator client if the connection is closed.
        /// Reconnection is needed in case of network issues or server rebooting.
        /// The method will try to reconnect to all the endpoints before giving up and throwing an exception.
        /// </summary>
        private async Task MayBeReconnectLocator()
        {
            // if we are here, at least one endpoint was reachable,
            // the first connection was successful,
            // but then the connection was closed.
            // We try to reconnect to all the endpoints before giving up.
            // We could have a load balancer in front of the cluster.
            var maxAttempts = _clientParameters.LookupLocatorStrategy.MaxAttempts;

            try
            {
                await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
                if (_client.IsClosed)
                {
                    var retryCount = 0;
                    var advId = Random.Shared.Next(0, _clientParameters.Endpoints.Count);
                    while (retryCount < maxAttempts)
                    {
                        try
                        {
                            retryCount++;
                            _client = await Client.Create(_client.Parameters with
                            {
                                ClientProvidedName = _clientParameters.ClientProvidedName,
                                Endpoint = _clientParameters.Endpoints[advId]
                            }).ConfigureAwait(false);
                            _logger?.LogDebug("Locator reconnected to {@EndPoint}", _clientParameters.Endpoints[advId]);
                            break;
                        }
                        catch (Exception e)
                        {
                            var delay = _clientParameters.LookupLocatorStrategy.Delay;
                            _logger?.LogError(e,
                                "Failed to reconnect locator to {@EndPoint}, will retry again. Retry count: {@retryCount} in {@Delay}",
                                _clientParameters.Endpoints[advId], retryCount, delay);
                            await Task.Delay(_clientParameters.LookupLocatorStrategy.Delay).ConfigureAwait(false);
                            advId = (advId + 1) % _clientParameters.Endpoints.Count;
                            if (retryCount >= maxAttempts)
                                throw;
                        }
                    }
                }
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        /// <summary>
        /// Stores an offset for a consumer on a stream. Used for offset tracking and recovery.
        /// </summary>
        /// <param name="reference">Consumer reference (name) that committed the offset.</param>
        /// <param name="stream">Stream name.</param>
        /// <param name="offsetValue">The offset value to store.</param>
        public async Task StoreOffset(string reference, string stream, ulong offsetValue)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            await _client.StoreOffset(reference, stream, offsetValue).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates the connection secret (password) for the stream system and all its producer/consumer connections.
        /// </summary>
        /// <param name="newSecret">The new password to use.</param>
        /// <exception cref="UpdateSecretFailureException">Thrown when the locator connection is closed and the update cannot be applied.</exception>
        public async Task UpdateSecret(string newSecret)
        {
            // store the old password just in case it will fail to update the secret
            var oldSecret = _clientParameters.Password;
            _clientParameters.Password = newSecret;
            _client.Parameters.Password = newSecret;
            await MayBeReconnectLocator().ConfigureAwait(false);
            if (_client.IsClosed)
            {
                // it can happen during some network problem or server rebooting
                // even the _clientParameters.Password could be invalid we restore the 
                // the old one just to be consistent 
                _clientParameters.Password = oldSecret;
                _client.Parameters.Password = oldSecret;
                throw new UpdateSecretFailureException("Cannot update a closed connection.");
            }

            await _client.UpdateSecret(newSecret).ConfigureAwait(false);
            await PoolConsumers.UpdateSecrets(newSecret).ConfigureAwait(false);
            await PoolProducers.UpdateSecrets(newSecret).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a raw super stream producer that publishes to a super stream using the given config and routing.
        /// </summary>
        /// <param name="rawSuperStreamProducerConfig">Producer configuration including super stream name and routing.</param>
        /// <param name="logger">Optional logger.</param>
        /// <returns>An <see cref="ISuperStreamProducer"/> for the super stream.</returns>
        /// <exception cref="CreateProducerException">Thrown when the super stream name is empty, batch size is invalid, routing is null, or metadata query fails.</exception>
        public async Task<ISuperStreamProducer> CreateRawSuperStreamProducer(
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

            foreach (var (_, value) in streamInfos)
            {
                ClientExceptions.CheckLeader(value);
            }

            await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
            try
            {
                var r = RawSuperStreamProducer.Create(rawSuperStreamProducerConfig,
                    streamInfos,
                    _clientParameters with { ClientProvidedName = rawSuperStreamProducerConfig.ClientProvidedName },
                    logger);
                _logger?.LogDebug("Raw Producer: {ProducerReference} created for SuperStream: {SuperStream}",
                    rawSuperStreamProducerConfig.Reference,
                    rawSuperStreamProducerConfig.SuperStream);
                return r;
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        /// <summary>
        /// Returns the list of partitions stream names for a given super stream.
        /// </summary>
        /// <param name="superStream">The super stream name.</param>
        /// <returns>Array of partition stream names.</returns>
        /// <exception cref="QueryException">Thrown when the metadata query fails.</exception>
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

        /// <summary>
        /// Creates a super stream consumer that consumes from all partitions of a super stream.
        /// </summary>
        /// <param name="rawSuperStreamConsumerConfig">Consumer configuration including super stream name and reference.</param>
        /// <param name="logger">Optional logger.</param>
        /// <returns>An <see cref="ISuperStreamConsumer"/> for the super stream.</returns>
        /// <exception cref="CreateConsumerException">Thrown when the super stream name is empty or metadata query fails.</exception>
        public async Task<ISuperStreamConsumer> CreateSuperStreamConsumer(
            RawSuperStreamConsumerConfig rawSuperStreamConsumerConfig,
            ILogger logger = null)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(rawSuperStreamConsumerConfig.SuperStream))
            {
                throw new CreateConsumerException("Super Stream name can't be empty");
            }

            rawSuperStreamConsumerConfig.Client = _client;
            rawSuperStreamConsumerConfig.Pool = PoolConsumers;

            var partitions = await _client.QueryPartition(rawSuperStreamConsumerConfig.SuperStream)
                .ConfigureAwait(false);
            if (partitions.ResponseCode != ResponseCode.Ok)
            {
                throw new CreateConsumerException($"consumer could not be created code: {partitions.ResponseCode}",
                    partitions.ResponseCode);
            }

            IDictionary<string, StreamInfo> streamInfos = new Dictionary<string, StreamInfo>();
            foreach (var partitionsStream in partitions.Streams)
            {
                streamInfos[partitionsStream] = await StreamInfo(partitionsStream).ConfigureAwait(false);
            }

            await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
            try
            {
                var s = RawSuperStreamConsumer.Create(rawSuperStreamConsumerConfig,
                    streamInfos,
                    _clientParameters with { ClientProvidedName = rawSuperStreamConsumerConfig.ClientProvidedName },
                    logger);
                _logger?.LogDebug("Consumer: {Reference} created for SuperStream: {SuperStream}",
                    rawSuperStreamConsumerConfig.Reference, rawSuperStreamConsumerConfig.SuperStream);

                return s;
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        /// <summary>
        /// Creates a raw producer for a single stream.
        /// </summary>
        /// <param name="rawProducerConfig">Producer configuration including stream name and batch size.</param>
        /// <param name="logger">Optional logger.</param>
        /// <returns>An <see cref="IProducer"/> for the stream.</returns>
        /// <exception cref="CreateProducerException">Thrown when batch size is invalid or stream metadata is unavailable.</exception>
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

            ClientExceptions.CheckLeader(metaStreamInfo);

            await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
            try
            {
                rawProducerConfig.Pool = PoolProducers;

                var s = _clientParameters with { ClientProvidedName = rawProducerConfig.ClientProvidedName };

                var p = await RawProducer.Create(s,
                    rawProducerConfig, metaStreamInfo, logger).ConfigureAwait(false);
                return p;
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        /// <summary>
        /// Retrieves metadata for a stream (leader and replicas). Used internally when creating producers/consumers.
        /// </summary>
        /// <param name="streamName">The stream name.</param>
        /// <returns>Stream metadata including response code and broker information.</returns>
        public async Task<StreamInfo> StreamInfo(string streamName)
        {
            // force localhost connection for single node clusters and when address resolver is not provided
            // when there is 1 endpoint and an address resolver, there could be a cluster behind a load balancer
            var forceLocalHost = false;
            var localPort = 0;
            var localHostOrAddress = "";
            if (_clientParameters.Endpoints.Count == 1 &&
                _clientParameters.AddressResolver is null)
            {
                var clientParametersEndpoint = _clientParameters.Endpoints[0];
                switch (clientParametersEndpoint)
                {
                    case DnsEndPoint { Host: "localhost" } dnsEndPoint:
                        forceLocalHost = true;
                        localPort = dnsEndPoint.Port;
                        localHostOrAddress = dnsEndPoint.Host;
                        break;
                    case IPEndPoint ipEndPoint when IPAddress.IsLoopback(ipEndPoint.Address):
                        forceLocalHost = true;
                        localPort = ipEndPoint.Port;
                        localHostOrAddress = ipEndPoint.Address.ToString();
                        break;
                }
            }

            StreamInfo metaStreamInfo;
            if (forceLocalHost)
            {
                // craft the metadata response to force using localhost
                var leader = new Broker(localHostOrAddress, (uint)localPort);
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

        /// <summary>
        /// Creates a stream with the given specification (name and optional arguments).
        /// </summary>
        /// <param name="spec">Stream specification (name and optional arguments).</param>
        /// <exception cref="CreateStreamException">Thrown when the server returns an error other than Ok or StreamAlreadyExists.</exception>
        public async Task CreateStream(StreamSpec spec)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var response = await _client.CreateStream(spec.Name, spec.Args).ConfigureAwait(false);
            if (response.ResponseCode is ResponseCode.Ok or ResponseCode.StreamAlreadyExists)
            {
                return;
            }

            throw new CreateStreamException($"Failed to create stream, error code: {response.ResponseCode.ToString()}");
        }

        /// <summary>
        /// Creates a super stream with the given specification (name, partitions, optional binding keys and arguments).
        /// </summary>
        /// <param name="spec">Super stream specification.</param>
        /// <exception cref="CreateStreamException">Thrown when the server returns an error other than Ok or StreamAlreadyExists.</exception>
        public async Task CreateSuperStream(SuperStreamSpec spec)
        {
            spec.Validate();
            await MayBeReconnectLocator().ConfigureAwait(false);
            var response = await _client
                .CreateSuperStream(spec.Name, spec.GetPartitions(), spec.GetBindingKeys(), spec.Args)
                .ConfigureAwait(false);
            if (response.ResponseCode is ResponseCode.Ok or ResponseCode.StreamAlreadyExists)
            {
                return;
            }

            throw new CreateStreamException($"Failed to create stream, error code: {response.ResponseCode.ToString()}");
        }

        /// <summary>
        /// Checks whether a stream exists.
        /// </summary>
        /// <param name="stream">Stream name.</param>
        /// <returns>True if the stream exists, false otherwise.</returns>
        public async Task<bool> StreamExists(string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
            try
            {
                return await _client.StreamExists(stream).ConfigureAwait(false);
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        /// <summary>
        /// Checks whether a super stream exists.
        /// </summary>
        /// <param name="superStream">Super stream name.</param>
        /// <returns>True if the super stream exists, false otherwise.</returns>
        public async Task<bool> SuperStreamExists(string superStream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
            try
            {
                return await _client.SuperStreamExists(superStream).ConfigureAwait(false);
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }

        private static void MaybeThrowQueryException(string reference, string stream)
        {
            if (string.IsNullOrWhiteSpace(reference) || string.IsNullOrWhiteSpace(stream))
            {
                throw new ArgumentException("Stream name and reference can't be empty or null");
            }
        }

        /// <summary>
        /// Retrieves the last consumer offset stored for the given consumer reference and stream.
        /// </summary>
        /// <param name="reference">Consumer reference (name).</param>
        /// <param name="stream">Stream name.</param>
        /// <returns>The last stored offset for the consumer on the stream.</returns>
        /// <exception cref="OffsetNotFoundException">Thrown when no offset has been stored for this consumer and stream.</exception>
        public async Task<ulong> QueryOffset(string reference, string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var offset = await TryQueryOffset(reference, stream).ConfigureAwait(false);
            return offset ??
                   throw new OffsetNotFoundException($"QueryOffset stream: {stream}, reference: {reference}");
        }

        /// <summary>
        /// Tries to retrieve the last consumer offset stored for the given consumer reference and stream.
        /// Returns null if no offset has been stored (avoids throwing <see cref="OffsetNotFoundException"/>).
        /// </summary>
        /// <param name="reference">Consumer reference (name).</param>
        /// <param name="stream">Stream name.</param>
        /// <returns>The last stored offset, or null if not found.</returns>
        public async Task<ulong?> TryQueryOffset(string reference, string stream)
        {
            MaybeThrowQueryException(reference, stream);
            await MayBeReconnectLocator().ConfigureAwait(false);

            var response = await _client.QueryOffset(reference, stream).ConfigureAwait(false);

            // Offset do not exist so just return null. There is no need to throw an OffsetNotFoundException and capture it.
            if (response.ResponseCode == ResponseCode.OffsetNotFound)
            {
                return null;
            }

            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"QueryOffset stream: {stream}, reference: {reference}");
            return response.Offset;
        }

        /// <summary>
        /// Retrieves the last publishing sequence (ID) for the given producer reference and stream.
        /// </summary>
        /// <param name="reference">Producer reference (name).</param>
        /// <param name="stream">Stream name.</param>
        /// <returns>The last published sequence number.</returns>
        public async Task<ulong> QuerySequence(string reference, string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            MaybeThrowQueryException(reference, stream);
            var response = await _client.QueryPublisherSequence(reference, stream).ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"QuerySequence stream: {stream}, reference: {reference}");
            return response.Sequence;
        }

        /// <summary>
        /// Deletes a stream and its data.
        /// </summary>
        /// <param name="stream">Stream name to delete.</param>
        /// <exception cref="DeleteStreamException">Thrown when the server returns an error.</exception>
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

        /// <summary>
        /// Deletes a super stream and all its partition streams.
        /// </summary>
        /// <param name="superStream">Super stream name to delete.</param>
        /// <exception cref="DeleteStreamException">Thrown when the server returns an error.</exception>
        public async Task DeleteSuperStream(string superStream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var response = await _client.DeleteSuperStream(superStream).ConfigureAwait(false);
            if (response.ResponseCode == ResponseCode.Ok)
            {
                return;
            }

            throw new DeleteStreamException(
                $"Failed to delete super stream, error code: {response.ResponseCode.ToString()}");
        }

        /// <summary>
        /// Retrieves statistics for a stream (e.g. first offset, last offset, length).
        /// </summary>
        /// <param name="stream">Stream name.</param>
        /// <returns>Stream statistics.</returns>
        public async Task<StreamStats> StreamStats(string stream)
        {
            await MayBeReconnectLocator().ConfigureAwait(false);
            var response = await _client.StreamStats(stream).ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(response.ResponseCode,
                $"StreamStats stream: {stream}");
            return new StreamStats(response.Statistic, stream);
        }

        /// <summary>
        /// Creates a raw consumer for a single stream.
        /// </summary>
        /// <param name="rawConsumerConfig">Consumer configuration including stream name and reference.</param>
        /// <param name="logger">Optional logger.</param>
        /// <returns>An <see cref="IConsumer"/> for the stream.</returns>
        /// <exception cref="CreateConsumerException">Thrown when the stream does not exist or metadata is unavailable.</exception>
        public async Task<IConsumer> CreateRawConsumer(RawConsumerConfig rawConsumerConfig,
            ILogger logger = null)
        {
            var metaStreamInfo = await StreamInfo(rawConsumerConfig.Stream).ConfigureAwait(false);
            if (metaStreamInfo.ResponseCode != ResponseCode.Ok)
            {
                throw new CreateConsumerException($"consumer could not be created code: {metaStreamInfo.ResponseCode}",
                    metaStreamInfo.ResponseCode);
            }

            ClientExceptions.CheckLeader(metaStreamInfo);

            try
            {
                await _semClientProvidedName.WaitAsync().ConfigureAwait(false);
                rawConsumerConfig.Pool = PoolConsumers;
                var s = _clientParameters with { ClientProvidedName = rawConsumerConfig.ClientProvidedName };
                return await RawConsumer.Create(s, rawConsumerConfig, metaStreamInfo, logger).ConfigureAwait(false);
            }
            finally
            {
                _semClientProvidedName.Release();
            }
        }
    }

    /// <summary>
    /// Base exception for entity creation failures (producer/consumer), optionally carrying a <see cref="ResponseCode"/>.
    /// </summary>
    public class CreateException : Exception
    {
        protected CreateException(string s, ResponseCode responseCode) : base(s)
        {
            ResponseCode = responseCode;
        }

        protected CreateException(string s) : base(s)
        {
        }

        public ResponseCode ResponseCode { get; init; }
    }

    /// <summary>Exception thrown when consumer creation fails.</summary>
    public class CreateConsumerException : CreateException
    {
        public CreateConsumerException(string s, ResponseCode responseCode) : base(s, responseCode)
        {
        }

        public CreateConsumerException(string s) : base(s)
        {
        }
    }

    /// <summary>Exception thrown when stream or super stream creation fails.</summary>
    public class CreateStreamException : Exception
    {
        public CreateStreamException(string s) : base(s)
        {
        }
    }

    /// <summary>Exception thrown when stream or super stream deletion fails.</summary>
    public class DeleteStreamException : Exception
    {
        public DeleteStreamException(string s) : base(s)
        {
        }
    }

    /// <summary>Exception thrown when a metadata or partition query fails.</summary>
    public class QueryException : Exception
    {
        public QueryException(string s) : base(s)
        {
        }
    }

    /// <summary>Exception thrown when producer creation fails.</summary>
    public class CreateProducerException : CreateException
    {
        public CreateProducerException(string s, ResponseCode responseCode) : base(s, responseCode)
        {
        }

        public CreateProducerException(string s) : base(s)
        {
        }
    }

    /// <summary>
    /// Strategy for choosing which stream replica (leader) to use when multiple are available.
    /// </summary>
    public readonly struct LeaderLocator
    {
        private readonly string _value;

        private LeaderLocator(string value)
        {
            _value = value;
        }

        /// <summary>Prefer the leader on the same node as the client connection.</summary>
        public static LeaderLocator ClientLocal => new("client-local");

        /// <summary>Choose a leader at random.</summary>
        public static LeaderLocator Random => new("random");

        /// <summary>Prefer the node that is currently leader for the fewest streams.</summary>
        public static LeaderLocator LeastLeaders => new("least-leaders");

        public override string ToString()
        {
            return _value;
        }
    }

    /// <summary>Exception thrown when <see cref="StreamSystem.Create"/> cannot connect to any configured endpoint.</summary>
    public class StreamSystemInitialisationException : Exception
    {
        public StreamSystemInitialisationException(string error) : base(error)
        {
        }
    }

    /// <summary>Exception thrown when <see cref="StreamSystem.UpdateSecret"/> fails (e.g. connection is closed).</summary>
    public class UpdateSecretFailureException : ProtocolException
    {
        public UpdateSecretFailureException(string s)
            : base(s)
        {
        }
    }
}
