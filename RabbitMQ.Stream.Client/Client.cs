// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace RabbitMQ.Stream.Client
{
    public enum AuthMechanism
    {
        Plain,
        External,
    }

    public record ClientParameters
    {
        // internal list of endpoints where the client will try to connect
        // the property Endpoint will use one of the endpoints in the list
        // we keep it separate from the Endpoint property to avoid confusion
        // only for internal user
        internal IList<EndPoint> Endpoints { get; set; } = new List<EndPoint>();

        private string _clientProvidedName;

        public IDictionary<string, string> Properties { get; } =
            new Dictionary<string, string>
            {
                { "product", "RabbitMQ Stream" },
                { "version", Version.VersionString },
                { "platform", ".NET" },
                {
                    "copyright",
                    "Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries."
                },
                {
                    "information",
                    "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/"
                },
                { "connection_name", "Unknown" }
            };

        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string VirtualHost { get; set; } = "/";
        public EndPoint Endpoint { get; set; } = new IPEndPoint(IPAddress.Loopback, 5552);

        public delegate Task MetadataUpdateHandler(MetaDataUpdate update);

        public event MetadataUpdateHandler OnMetadataUpdate;
        public TimeSpan Heartbeat { get; set; } = TimeSpan.FromMinutes(1);

        public string ClientProvidedName
        {
            get => _clientProvidedName ??= Properties["connection_name"];
            set => _clientProvidedName = Properties["connection_name"] = value;
        }

        /// <summary>
        /// TLS options setting.
        /// </summary>
        public SslOption Ssl { get; set; } = new SslOption();

        public AddressResolver AddressResolver { get; set; } = null;

        public AuthMechanism AuthMechanism { get; set; } = AuthMechanism.Plain;

        public TimeSpan RpcTimeOut { get; set; } = TimeSpan.FromSeconds(10);

        internal void FireMetadataUpdate(MetaDataUpdate metaDataUpdate)
        {
            OnMetadataUpdate?.Invoke(metaDataUpdate);
        }
    }

    internal readonly struct OutgoingMsg : ICommand
    {
        private readonly byte publisherId;
        private readonly ulong publishingId;
        private readonly Message data;

        public OutgoingMsg(byte publisherId, ulong publishingId, Message data)
        {
            this.publisherId = publisherId;
            this.publishingId = publishingId;
            this.data = data;
        }

        public byte PublisherId => publisherId;

        public ulong PublishingId => publishingId;

        public Message Data => data;
        public int SizeNeeded => 0;

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }

    public class Client : IClient
    {
        private bool isClosed = true;

        private uint correlationId = 0; // allow for some pre-amble

        private Connection _connection;

        private readonly ConcurrentDictionary<uint, IValueTaskSource> requests = new();

        private readonly TaskCompletionSource<TuneResponse> tuneReceived =
            new TaskCompletionSource<TuneResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal readonly IDictionary<byte, (string, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>))>
            publishers =
                new ConcurrentDictionary<byte, (string, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>)
                    )>();

        internal readonly IDictionary<byte, (string, ConsumerEvents)> consumers =
            new ConcurrentDictionary<byte, (string, ConsumerEvents)>();

        private int publishCommandsSent;

        private readonly HeartBeatHandler _heartBeatHandler;

        public int PublishCommandsSent => publishCommandsSent;

        public int MessagesSent => messagesSent;
        public uint MaxFrameSize => tuneReceived.Task.Result.FrameMax;

        private int messagesSent;
        private int confirmFrames;
        public IDictionary<string, string> ConnectionProperties { get; private set; }
        public ClientParameters Parameters { get; set; }

        public int ConfirmFrames => confirmFrames;

        public int IncomingFrames => _connection.NumFrames;

        private static readonly object Obj = new();

        private readonly ILogger _logger;

        private Client(ClientParameters parameters, ILogger logger = null)
        {
            Parameters = parameters;
            _heartBeatHandler = new HeartBeatHandler(
                SendHeartBeat,
                Close,
                (int)parameters.Heartbeat.TotalSeconds);
            IsClosed = false;
            _logger = logger ?? NullLogger.Instance;
            ClientId = Guid.NewGuid().ToString();
        }

        public bool IsClosed
        {
            get
            {
                if (_connection.IsClosed)
                {
                    isClosed = true;
                }

                return isClosed;
            }

            private set => isClosed = value;
        }

        private void StartHeartBeat()
        {
            _heartBeatHandler.Start();
        }

        public delegate Task ConnectionCloseHandler(string reason);

        public event ConnectionCloseHandler ConnectionClosed;

        private async Task OnConnectionClosed(string reason)
        {
            if (ConnectionClosed != null)
            {
                var t = ConnectionClosed?.Invoke(reason)!;
                if (t != null)
                    await t.ConfigureAwait(false);
            }
        }

        public static async Task<Client> Create(ClientParameters parameters, ILogger logger = null)
        {
            var client = new Client(parameters, logger);
            client._connection = await Connection
                .Create(parameters.Endpoint, client.HandleIncoming, client.HandleClosed, parameters.Ssl, logger)
                .ConfigureAwait(false);
            client._connection.ClientId = client.ClientId;
            // exchange properties
            var peerPropertiesResponse = await client.Request<PeerPropertiesRequest, PeerPropertiesResponse>(corr =>
                new PeerPropertiesRequest(corr, parameters.Properties)).ConfigureAwait(false);
            logger?.LogDebug("Server properties: {@Properties}", peerPropertiesResponse.Properties);

            //auth
            var saslHandshakeResponse =
                await client
                    .Request<SaslHandshakeRequest, SaslHandshakeResponse>(corr => new SaslHandshakeRequest(corr))
                    .ConfigureAwait(false);
            logger?.LogDebug("Sasl mechanism: {Mechanisms}", saslHandshakeResponse.Mechanisms);

            var isValid = saslHandshakeResponse.Mechanisms.Contains(
                parameters.AuthMechanism.ToString().ToUpperInvariant(),
                StringComparer.OrdinalIgnoreCase);
            if (!isValid)
            {
                throw new AuthMechanismNotSupportedException(
                    $"Sasl mechanism {parameters.AuthMechanism} is not supported by the server");
            }

            var saslData = Encoding.UTF8.GetBytes($"\0{parameters.UserName}\0{parameters.Password}");
            var authResponse =
                await client
                    .Request<SaslAuthenticateRequest, SaslAuthenticateResponse>(corr =>
                        new SaslAuthenticateRequest(corr, parameters.AuthMechanism.ToString().ToUpperInvariant(),
                            saslData))
                    .ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(authResponse.ResponseCode, parameters.UserName);

            //tune
            await client.tuneReceived.Task.ConfigureAwait(false);
            await client.Publish(new TuneRequest(0,
                (uint)client.Parameters.Heartbeat.TotalSeconds)).ConfigureAwait(false);

            // open 
            var open = await client
                .Request<OpenRequest, OpenResponse>(corr => new OpenRequest(corr, parameters.VirtualHost))
                .ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(open.ResponseCode, parameters.VirtualHost);
            logger?.LogDebug("Open: ConnectionProperties: {ConnectionProperties}", open.ConnectionProperties);
            client.ConnectionProperties = open.ConnectionProperties;

            if (peerPropertiesResponse.Properties.TryGetValue("version", out var version))
            {
                AvailableFeaturesSingleton.Instance.SetServerVersion(version);
                logger?.LogDebug("Extracted BrokerVersion version: {Version}",
                    AvailableFeaturesSingleton.Instance.BrokerVersion);
                if (AvailableFeaturesSingleton.Instance.Is311OrMore)
                {
                    var features = await client.ExchangeVersions().ConfigureAwait(false);
                    AvailableFeaturesSingleton.Instance.ParseCommandVersions(features.Commands);
                }
                else
                {
                    logger?.LogInformation(
                        "Server version is less than 3.11.0, skipping command version exchange");
                }
            }
            else
            {
                logger?.LogInformation(
                    "Server version is less than 3.11.0, skipping command version exchange");
            }

            client.correlationId = 100;
            // start heart beat only when the client is connected
            client.StartHeartBeat();
            return client;
        }

        public async Task UpdateSecret(string newSecret)
        {
            var saslData = Encoding.UTF8.GetBytes($"\0{Parameters.UserName}\0{newSecret}");

            var authResponse =
                await Request<SaslAuthenticateRequest, SaslAuthenticateResponse>(corr =>
                        new SaslAuthenticateRequest(
                            corr,
                            Parameters.AuthMechanism.ToString().ToUpperInvariant(),
                            saslData))
                    .ConfigureAwait(false);

            ClientExceptions.MaybeThrowException(
                authResponse.ResponseCode,
                "Error while updating secret: the secret will not be updated.");
        }

        public async ValueTask<bool> Publish(Publish publishMsg)
        {
            var publishTask = await Publish<Publish>(publishMsg).ConfigureAwait(false);

            publishCommandsSent += 1;
            messagesSent += publishMsg.MessageCount;
            return publishTask;
        }

        public ValueTask<bool> Publish<T>(T msg) where T : struct, ICommand
        {
            try
            {
                return _connection.Write(msg);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "An error occurred while writing the buffer to the socket");
                throw;
            }
        }

        public async Task<(byte, DeclarePublisherResponse)> DeclarePublisher(string publisherRef,
            string stream,
            Action<ReadOnlyMemory<ulong>> confirmCallback,
            Action<(ulong, ResponseCode)[]> errorCallback, ConnectionsPool pool = null)
        {
            await _poolSemaphore.WaitAsync().ConfigureAwait(false);
            var publisherId = ConnectionsPool.FindNextValidId(publishers.Keys.ToList(), IncrementEntityId());
            DeclarePublisherResponse response;

            try
            {
                publishers.Add(publisherId, (stream,
                    (confirmCallback, errorCallback)));
                response = await Request<DeclarePublisherRequest, DeclarePublisherResponse>(corr =>
                    new DeclarePublisherRequest(corr, publisherId, publisherRef, stream)).ConfigureAwait(false);
            }
            finally
            {
                _poolSemaphore.Release();
            }

            if (response.ResponseCode == ResponseCode.Ok)
                return (publisherId, response);

            // and close the connection if necessary. 
            publishers.Remove(publisherId);
            pool?.MaybeClose(ClientId, "Publisher creation failed");
            return (publisherId, response);
        }

        public async Task<DeletePublisherResponse> DeletePublisher(byte publisherId,
            bool ignoreIfAlreadyRemoved = false)
        {
            await _poolSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!ignoreIfAlreadyRemoved)
                {
                    var result =
                        await Request<DeletePublisherRequest, DeletePublisherResponse>(corr =>
                            new DeletePublisherRequest(corr, publisherId)).ConfigureAwait(false);

                    return result;
                }
            }
            finally
            {
                publishers.Remove(publisherId);
                _poolSemaphore.Release();
            }

            return new DeletePublisherResponse();
        }

        public async Task<(byte, SubscribeResponse)> Subscribe(string stream, IOffsetType offsetType,
            ushort initialCredit,
            Dictionary<string, string> properties, Func<Deliver, Task> deliverHandler,
            Func<bool, Task<IOffsetType>> consumerUpdateHandler = null, ConnectionsPool pool = null)
        {
            return await Subscribe(new RawConsumerConfig(stream) { OffsetSpec = offsetType, Pool = pool },
                initialCredit,
                properties,
                deliverHandler,
                consumerUpdateHandler).ConfigureAwait(false);
        }

        private byte _nextEntityId = 0;

        // the entity id is a byte so we need to increment it and reset it when it reaches the max value
        // to avoid to use always the same ids when producers and consumers are created 
        // so even there is a connection with one producer or consumer we need to increment the id 
        private byte IncrementEntityId()
        {
            lock (Obj)
            {
                var current = _nextEntityId;
                _nextEntityId++;
                if (_nextEntityId != byte.MaxValue)
                    return current;
                _nextEntityId = 0;
                return _nextEntityId;
            }
        }

        public async Task<(byte, SubscribeResponse)> Subscribe(RawConsumerConfig config,
            ushort initialCredit,
            Dictionary<string, string> properties, Func<Deliver, Task> deliverHandler,
            Func<bool, Task<IOffsetType>> consumerUpdateHandler)
        {
            await _poolSemaphore.WaitAsync().ConfigureAwait(false);
            var subscriptionId = ConnectionsPool.FindNextValidId(consumers.Keys.ToList(), IncrementEntityId());
            SubscribeResponse response;
            try
            {
                consumers.Add(subscriptionId,
                    (config.Stream,
                        new ConsumerEvents(
                            deliverHandler,
                            consumerUpdateHandler)));

                response = await Request<SubscribeRequest, SubscribeResponse>(corr =>
                    new SubscribeRequest(corr, subscriptionId, config.Stream, config.OffsetSpec, initialCredit,
                        properties)).ConfigureAwait(false);
            }
            finally
            {
                _poolSemaphore.Release();
            }

            if (response.ResponseCode == ResponseCode.Ok)
                return (subscriptionId, response);

            consumers.Remove(subscriptionId);
            config.Pool.MaybeClose(ClientId, "Subscription failed");
            return (subscriptionId, response);
        }

        public async Task<UnsubscribeResponse> Unsubscribe(byte subscriptionId, bool ignoreIfAlreadyRemoved = false)
        {
            await _poolSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!ignoreIfAlreadyRemoved)
                {
                    // here we reduce a bit the timeout to avoid waiting too much
                    // if the client is busy with read operations it can take time to process the unsubscribe
                    // but the subscribe is removed.
                    var result =
                        await Request<UnsubscribeRequest, UnsubscribeResponse>(corr =>
                                new UnsubscribeRequest(corr, subscriptionId),
                            TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                    _logger.LogDebug("Unsubscribe request : {SubscriptionId}", subscriptionId);

                    return result;
                }
            }
            finally
            {
                _logger.LogDebug("Unsubscribe: {SubscriptionId}", subscriptionId);
                // remove consumer after RPC returns, this should avoid uncorrelated data being sent
                consumers.Remove(subscriptionId);
                _poolSemaphore.Release();
            }

            return new UnsubscribeResponse();
        }

        public async Task<PartitionsQueryResponse> QueryPartition(string superStream)
        {
            return await Request<PartitionsQueryRequest, PartitionsQueryResponse>(corr =>
                new PartitionsQueryRequest(corr, superStream)).ConfigureAwait(false);
        }

        public async Task<RouteQueryResponse> QueryRoute(string superStream, string routingKey)
        {
            return await Request<RouteQueryRequest, RouteQueryResponse>(corr =>
                new RouteQueryRequest(corr, superStream, routingKey)).ConfigureAwait(false);
        }

        public async Task<CommandVersionsResponse> ExchangeVersions()
        {
            return await Request<CommandVersionsRequest, CommandVersionsResponse>(corr =>
                new CommandVersionsRequest(corr)).ConfigureAwait(false);
        }

        private async ValueTask<TOut> Request<TIn, TOut>(Func<uint, TIn> request, TimeSpan? timeout = null)
            where TIn : struct, ICommand where TOut : struct, ICommand
        {
            var corr = NextCorrelationId();
            var tcs = PooledTaskSource<TOut>.Rent();
            requests.TryAdd(corr, tcs);
            await Publish(request(corr)).ConfigureAwait(false);
            using var cts = new CancellationTokenSource(timeout ?? Parameters.RpcTimeOut);
            await using (cts.Token.Register(
                             valueTaskSource =>
                                 ((ManualResetValueTaskSource<TOut>)valueTaskSource).SetException(
                                     new TimeoutException()), tcs).ConfigureAwait(false))
            {
                var valueTask = new ValueTask<TOut>(tcs, tcs.Version);
                var result = await valueTask.ConfigureAwait(false);
                PooledTaskSource<TOut>.Return(tcs);
                return result;
            }
        }

        private uint NextCorrelationId()
        {
            return Interlocked.Increment(ref correlationId);
        }

        private async Task HandleClosed(string reason)
        {
            InternalClose();
            await OnConnectionClosed(reason).ConfigureAwait(false);
        }

        private async Task HandleIncoming(Memory<byte> frameMemory)
        {
            var frame = new ReadOnlySequence<byte>(frameMemory);
            WireFormatting.ReadUInt16(frame, out var tag);
            if ((tag & 0x8000) != 0)
            {
                tag = (ushort)(tag ^ 0x8000);
            }

            // in general every action updates the heartbeat server side
            // so there is no need to send the heartbeat when not necessary 
            _heartBeatHandler.UpdateHeartBeat();

            switch (tag)
            {
                case PublishConfirm.Key:
                    PublishConfirm.Read(frame, out var confirm);
                    confirmFrames += 1;
                    if (publishers.TryGetValue(confirm.PublisherId, out var publisherConf))
                    {
                        var (_, (confirmCallback, _)) = (publisherConf);

                        confirmCallback(confirm.PublishingIds);
                        if (MemoryMarshal.TryGetArray(confirm.PublishingIds, out var confirmSegment))
                        {
                            if (confirmSegment.Array != null)
                                ArrayPool<ulong>.Shared.Return(confirmSegment.Array);
                        }
                    }
                    else
                    {
                        // the producer is not found, this can happen when the producer is closing
                        // and there are still confirmation on the wire 
                        // we can ignore the error since the producer does not exists anymore
                        _logger?.LogDebug(
                            "Could not find stream producer {ID} or producer is closing." +
                            "A possible cause it that the producer was closed and the are still confirmation on the wire. ",
                            confirm.PublisherId);
                    }

                    break;
                case Deliver.Key:
                    Deliver.Read(frame, out var deliver);
                    if (consumers.TryGetValue(deliver.SubscriptionId, out var consumerEvent))
                    {
                        var (_, deliverHandler) = consumerEvent;
                        await deliverHandler.DeliverHandler(deliver).ConfigureAwait(false);
                    }
                    else
                    {
                        // the consumer is not found, this can happen when the consumer is closing
                        // and there are still chunks on the wire to the handler is still processing the chunks
                        // we can ignore the chunk since the subscription does not exists anymore
                        _logger?.LogDebug(
                            "Could not find stream subscription {ID} or subscription closing." +
                            "A possible cause it that the subscription was closed and the are still chunks on the wire. " +
                            "Reduce the initial credits can help to avoid this situation",
                            deliver.SubscriptionId);
                    }

                    break;
                case PublishError.Key:
                    PublishError.Read(frame, out var error);
                    if (publishers.TryGetValue(error.PublisherId, out var publisher))
                    {
                        var (_, (_, errorCallback)) = publisher;
                        errorCallback(error.PublishingErrors);
                    }
                    else
                    {
                        // the producer is not found, this can happen when the producer is closing
                        // and there are still confirmation on the wire 
                        // we can ignore the error since the producer does not exists anymore
                        _logger?.LogDebug(
                            "Could not find stream producer {ID} or producer is closing." +
                            "A possible cause it that the producer was closed and the are still confirmation on the wire. ",
                            error.PublisherId);
                    }

                    break;
                case MetaDataUpdate.Key:
                    MetaDataUpdate.Read(frame, out var metaDataUpdate);
                    Parameters.FireMetadataUpdate(metaDataUpdate);
                    break;
                case TuneResponse.Key:
                    TuneResponse.Read(frame, out var tuneResponse);
                    tuneReceived.SetResult(tuneResponse);
                    break;
                case ConsumerUpdateQueryResponse.Key:
                    ConsumerUpdateQueryResponse.Read(frame, out var consumerUpdateQueryResponse);
                    HandleCorrelatedResponse(consumerUpdateQueryResponse);
                    var consumerEventsUpd = consumers[consumerUpdateQueryResponse.SubscriptionId];
                    var consumer = consumerEventsUpd.Item2;
                    var off = await consumer.ConsumerUpdateHandler(consumerUpdateQueryResponse.IsActive)
                        .ConfigureAwait(false);
                    if (off == null)
                    {
                        _logger?.LogWarning(
                            "ConsumerUpdateHandler can't returned null, a default offsetType (OffsetTypeNext) will be used");
                        off = new OffsetTypeNext();
                    }

                    // event the consumer is not active, we need to send a ConsumerUpdateResponse
                    // by protocol definition. the offsetType can't be null so we use OffsetTypeNext as default
                    await ConsumerUpdateResponse(
                        consumerUpdateQueryResponse.CorrelationId,
                        off).ConfigureAwait(false);
                    break;
                case CreditResponse.Key:
                    CreditResponse.Read(frame, out var creditResponse);
                    creditResponse.HandleUnRoutableCredit(_logger);
                    break;
                default:
                    HandleCorrelatedCommand(tag, ref frame);
                    break;
            }

            if (MemoryMarshal.TryGetArray(frameMemory, out ArraySegment<byte> segment))
            {
                if (segment.Array != null)
                {
                    ArrayPool<byte>.Shared.Return(segment.Array);
                }
            }
        }

        private void HandleCorrelatedCommand(ushort tag, ref ReadOnlySequence<byte> frame)
        {
            switch (tag)
            {
                case DeclarePublisherResponse.Key:
                    DeclarePublisherResponse.Read(frame, out var declarePublisherResponse);
                    HandleCorrelatedResponse(declarePublisherResponse);
                    break;
                case QueryPublisherResponse.Key:
                    QueryPublisherResponse.Read(frame, out var queryPublisherResponse);
                    HandleCorrelatedResponse(queryPublisherResponse);
                    break;
                case DeletePublisherResponse.Key:
                    DeletePublisherResponse.Read(frame, out var deletePublisherResponse);
                    HandleCorrelatedResponse(deletePublisherResponse);
                    break;
                case SubscribeResponse.Key:
                    SubscribeResponse.Read(frame, out var subscribeResponse);
                    HandleCorrelatedResponse(subscribeResponse);
                    break;
                case QueryOffsetResponse.Key:
                    QueryOffsetResponse.Read(frame, out var queryOffsetResponse);
                    HandleCorrelatedResponse(queryOffsetResponse);
                    break;
                case StreamStatsResponse.Key:
                    StreamStatsResponse.Read(frame, out var streamStatsResponse);
                    HandleCorrelatedResponse(streamStatsResponse);
                    break;
                case UnsubscribeResponse.Key:
                    UnsubscribeResponse.Read(frame, out var unsubscribeResponse);
                    HandleCorrelatedResponse(unsubscribeResponse);
                    break;
                case CreateResponse.Key:
                    CreateResponse.Read(frame, out var createResponse);
                    HandleCorrelatedResponse(createResponse);
                    break;
                case DeleteResponse.Key:
                    DeleteResponse.Read(frame, out var deleteResponse);
                    HandleCorrelatedResponse(deleteResponse);
                    break;
                case MetaDataResponse.Key:
                    MetaDataResponse.Read(frame, out var metaDataResponse);
                    HandleCorrelatedResponse(metaDataResponse);
                    break;
                case PeerPropertiesResponse.Key:
                    PeerPropertiesResponse.Read(frame, out var peerPropertiesResponse);
                    HandleCorrelatedResponse(peerPropertiesResponse);
                    break;
                case SaslHandshakeResponse.Key:
                    SaslHandshakeResponse.Read(frame, out var saslHandshakeResponse);
                    HandleCorrelatedResponse(saslHandshakeResponse);
                    break;
                case SaslAuthenticateResponse.Key:
                    SaslAuthenticateResponse.Read(frame, out var saslAuthenticateResponse);
                    HandleCorrelatedResponse(saslAuthenticateResponse);
                    break;
                case OpenResponse.Key:
                    OpenResponse.Read(frame, out var openResponse);
                    HandleCorrelatedResponse(openResponse);
                    break;
                case CloseResponse.Key:
                    CloseResponse.Read(frame, out var closeResponse);
                    HandleCorrelatedResponse(closeResponse);
                    InternalClose();
                    break;
                case HeartBeatHandler.Key:
                    _heartBeatHandler.UpdateHeartBeat();
                    break;
                case PartitionsQueryResponse.Key:
                    PartitionsQueryResponse.Read(frame, out var partitionsQueryResponse);
                    HandleCorrelatedResponse(partitionsQueryResponse);
                    break;
                case RouteQueryResponse.Key:
                    RouteQueryResponse.Read(frame, out var routeQueryResponse);
                    HandleCorrelatedResponse(routeQueryResponse);
                    break;
                case CommandVersionsResponse.Key:
                    CommandVersionsResponse.Read(frame, out var commandVersionsResponse);
                    HandleCorrelatedResponse(commandVersionsResponse);
                    break;
                case CreateSuperStreamResponse.Key:
                    CreateSuperStreamResponse.Read(frame, out var superStreamResponse);
                    HandleCorrelatedResponse(superStreamResponse);
                    break;
                case DeleteSuperStreamResponse.Key:
                    DeleteSuperStreamResponse.Read(frame, out var deleteSuperStreamResponse);
                    HandleCorrelatedResponse(deleteSuperStreamResponse);
                    break;
                default:
                    if (MemoryMarshal.TryGetArray(frame.First, out var segment))
                    {
                        if (segment.Array != null)
                            ArrayPool<byte>.Shared.Return(segment.Array);
                    }

                    throw new ArgumentException($"Unknown or unexpected tag: {tag}", nameof(tag));
            }
        }

        private void HandleCorrelatedResponse<T>(T command) where T : struct, ICommand
        {
            if (command.CorrelationId == uint.MaxValue)
            {
                throw new UnknownCommandException($"unhandled incoming command {command.GetType()}");
            }

            if (requests.TryRemove(command.CorrelationId, out var tsc))
            {
                ((ManualResetValueTaskSource<T>)tsc).SetResult(command);
            }
        }

        private async ValueTask<bool> SendHeartBeat()
        {
            return await Publish(new HeartBeatRequest()).ConfigureAwait(false);
        }

        private void InternalClose()
        {
            _heartBeatHandler.Close();
            IsClosed = true;
        }

        private async ValueTask<bool> ConsumerUpdateResponse(uint rCorrelationId, IOffsetType offsetSpecification)
        {
            return await Publish(new ConsumerUpdateRequest(rCorrelationId, offsetSpecification)).ConfigureAwait(false);
        }

        private async Task<CloseResponse> Close(string reason, string closedStatus)
        {
            if (IsClosed)
            {
                return new CloseResponse(0, ResponseCode.Ok);
            }

            InternalClose();
            try
            {
                _connection.UpdateCloseStatus(closedStatus);
                var result =
                    await Request<CloseRequest, CloseResponse>(corr => new CloseRequest(corr, reason),
                        TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                return result;
            }
            catch (TimeoutException)
            {
                _logger.LogError(
                    "Timeout while closing the connection. The connection will be closed anyway");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "An error occurred while calling {CalledFunction}", nameof(_connection.Dispose));
            }
            finally
            {
                _connection.Dispose();
            }

            return new CloseResponse(0, ResponseCode.Ok);
        }

        public async Task<CloseResponse> Close(string reason)
        {
            return await Close(reason, ConnectionClosedReason.Normal).ConfigureAwait(false);
        }

        // _poolSemaphore is introduced here: https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/328 
        // the MaybeClose can be called in different threads so we need to protect the pool
        // the pool itself is thread safe but we need to protect the flow to be sure that the 
        // connection is released only once
        private readonly SemaphoreSlim _poolSemaphore = new(1, 1);

        // Safe close 
        // the client can be closed only if HasEntities is false
        // if the client has entities (publishers or consumers) it will be released from the pool
        // Release will decrement the active ids for the connection
        // if the active ids are 0 the connection will be closed

        internal async Task<CloseResponse> MaybeClose(string reason, ConnectionsPool pool)
        {
            await _poolSemaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                // the client can be closed in an unexpected way so we need to remove it from the pool
                // so you will find pool.remove(ClientId) also to the disconnect event
                pool.MaybeClose(ClientId, reason);
                var result = new CloseResponse(0, ResponseCode.Ok);
                return result;
            }
            finally
            {
                _poolSemaphore.Release();
            }
        }

        public string ClientId { get; init; }

        public IDictionary<byte, (string, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>))> Publishers
        {
            get => publishers;
        }

        public IDictionary<byte, (string, ConsumerEvents)> Consumers
        {
            get => consumers;
        }

        public async ValueTask<QueryPublisherResponse> QueryPublisherSequence(string publisherRef, string stream)
        {
            return await Request<QueryPublisherRequest, QueryPublisherResponse>(corr =>
                new QueryPublisherRequest(corr, publisherRef, stream)).ConfigureAwait(false);
        }

        public async ValueTask<bool> StoreOffset(string reference, string stream, ulong offsetValue)
        {
            return await Publish(new StoreOffsetRequest(stream, reference, offsetValue)).ConfigureAwait(false);
        }

        public async ValueTask<MetaDataResponse> QueryMetadata(string[] streams)
        {
            return await Request<MetaDataQuery, MetaDataResponse>(corr => new MetaDataQuery(corr, streams.ToList()))
                .ConfigureAwait(false);
        }

        public async Task<bool> SuperStreamExists(string stream)
        {
            var response = await QueryPartition(stream).ConfigureAwait(false);
            if (response is { Streams.Length: >= 0 } &&
                response.ResponseCode == ResponseCode.StreamNotAvailable)
            {
                ClientExceptions.MaybeThrowException(ResponseCode.StreamNotAvailable, stream);
            }

            return response is { Streams.Length: >= 0 } &&
                   response.ResponseCode == ResponseCode.Ok;
        }

        public async Task<bool> StreamExists(string stream)
        {
            var streams = new[] { stream };
            var response = await QueryMetadata(streams).ConfigureAwait(false);
            if (response.StreamInfos is { Count: >= 1 } &&
                response.StreamInfos[stream].ResponseCode == ResponseCode.StreamNotAvailable)
            {
                ClientExceptions.MaybeThrowException(ResponseCode.StreamNotAvailable, stream);
            }

            return response.StreamInfos is { Count: >= 1 } &&
                   response.StreamInfos[stream].ResponseCode == ResponseCode.Ok;
        }

        public async ValueTask<QueryOffsetResponse> QueryOffset(string reference, string stream)
        {
            return await Request<QueryOffsetRequest, QueryOffsetResponse>(corr =>
                new QueryOffsetRequest(stream, corr, reference)).ConfigureAwait(false);
        }

        public async ValueTask<CreateResponse> CreateStream(string stream, IDictionary<string, string> args)
        {
            return await Request<CreateRequest, CreateResponse>(corr => new CreateRequest(corr, stream, args))
                .ConfigureAwait(false);
        }

        public async ValueTask<DeleteResponse> DeleteStream(string stream)
        {
            return await Request<DeleteRequest, DeleteResponse>(corr => new DeleteRequest(corr, stream))
                .ConfigureAwait(false);
        }

        public async ValueTask<CreateSuperStreamResponse> CreateSuperStream(string superStream, List<string> partitions,
            List<string> bindingKeys, IDictionary<string, string> args)
        {
            return await Request<CreateSuperStreamRequest, CreateSuperStreamResponse>(corr =>
                    new CreateSuperStreamRequest(corr, superStream, partitions, bindingKeys, args))
                .ConfigureAwait(false);
        }

        public async ValueTask<DeleteSuperStreamResponse> DeleteSuperStream(string superStream)
        {
            return await Request<DeleteSuperStreamRequest, DeleteSuperStreamResponse>(corr =>
                    new DeleteSuperStreamRequest(corr, superStream))
                .ConfigureAwait(false);
        }

        public async ValueTask<bool> Credit(byte subscriptionId, ushort credit)
        {
            return await Publish(new CreditRequest(subscriptionId, credit)).ConfigureAwait(false);
        }

        public async ValueTask<StreamStatsResponse> StreamStats(string stream)
        {
            return await Request<StreamStatsRequest, StreamStatsResponse>(corr =>
                new StreamStatsRequest(corr, stream)).ConfigureAwait(false);
        }
    }

    internal static class PooledTaskSource<T>
    {
        private static readonly ConcurrentStack<ManualResetValueTaskSource<T>> stack =
            new ConcurrentStack<ManualResetValueTaskSource<T>>();

        public static ManualResetValueTaskSource<T> Rent()
        {
            if (stack.TryPop(out var task))
            {
                return task;
            }
            else
            {
                return new ManualResetValueTaskSource<T>() { RunContinuationsAsynchronously = true };
            }
        }

        public static void Return(ManualResetValueTaskSource<T> task)
        {
            task.Reset();
            stack.Push(task);
        }
    }

    internal sealed class ManualResetValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<T> _logic; // mutable struct; do not make this readonly

        public bool RunContinuationsAsynchronously
        {
            get => _logic.RunContinuationsAsynchronously;
            set => _logic.RunContinuationsAsynchronously = value;
        }

        public short Version => _logic.Version;
        public void Reset() => _logic.Reset();
        public void SetResult(T result) => _logic.SetResult(result);

        public void SetException(Exception error)
        {
            // https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/issues/384
            try
            {
                _logic.SetException(error);
            }
            catch (InvalidOperationException)
            {
            }
        }

        void IValueTaskSource.GetResult(short token) => _logic.GetResult(token);
        T IValueTaskSource<T>.GetResult(short token) => _logic.GetResult(token);
        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _logic.GetStatus(token);
        ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => _logic.GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object> continuation, object state, short token,
            ValueTaskSourceOnCompletedFlags flags) => _logic.OnCompleted(continuation, state, token, flags);

        void IValueTaskSource<T>.OnCompleted(Action<object> continuation, object state, short token,
            ValueTaskSourceOnCompletedFlags flags) => _logic.OnCompleted(continuation, state, token, flags);
    }
}
