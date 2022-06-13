// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace RabbitMQ.Stream.Client
{
    public record ClientParameters
    {
        private string _clientProvidedName;

        public IDictionary<string, string> Properties { get; } =
            new Dictionary<string, string>
            {
                {"product", "RabbitMQ Stream"},
                {"version", Version.VersionString},
                {"platform", ".NET"},
                {"copyright", "Copyright (c) 2020-2021 VMware, Inc. or its affiliates."},
                {
                    "information",
                    "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/"
                },
                {"connection_name", "Unknown"}
            };

        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string VirtualHost { get; set; } = "/";
        public EndPoint Endpoint { get; set; } = new IPEndPoint(IPAddress.Loopback, 5552);
        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };
        public Action<Exception> UnhandledExceptionHandler { get; set; } = _ => { };
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
    }

    public readonly struct OutgoingMsg : ICommand
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
        private bool isClosed = false;

        private readonly TimeSpan defaultTimeout = TimeSpan.FromSeconds(10);

        private uint correlationId = 0; // allow for some pre-amble

        private byte nextPublisherId = 0;

        private Connection connection;

        private readonly IDictionary<byte, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>)>
            publishers =
                new ConcurrentDictionary<byte, (Action<ReadOnlyMemory<ulong>>, Action<(ulong, ResponseCode)[]>)>();

        private readonly ConcurrentDictionary<uint, IValueTaskSource> requests = new();

        private readonly TaskCompletionSource<TuneResponse> tuneReceived =
            new TaskCompletionSource<TuneResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        private byte nextSubscriptionId;

        private readonly IDictionary<byte, Func<Deliver, Task>> consumers =
            new ConcurrentDictionary<byte, Func<Deliver, Task>>();

        private int publishCommandsSent;

        private readonly HeartBeatHandler _heartBeatHandler;

        public int PublishCommandsSent => publishCommandsSent;

        public int MessagesSent => messagesSent;

        private int messagesSent;
        private int confirmFrames;
        public IDictionary<string, string> ConnectionProperties { get; private set; }
        public ClientParameters Parameters { get; set; }

        public int ConfirmFrames => confirmFrames;

        public int IncomingFrames => connection.NumFrames;

        //public int IncomingChannelCount => this.incoming.Reader.Count;
        private static readonly object Obj = new();

        private byte GetNextSubscriptionId()
        {
            byte result;
            lock (Obj)
            {
                result = nextSubscriptionId++;
            }

            return result;
        }

        public bool IsClosed
        {
            get
            {
                if (connection.IsClosed)
                {
                    isClosed = true;
                }

                return isClosed;
            }

            private set => isClosed = value;
        }

        private Client(ClientParameters parameters)
        {
            Parameters = parameters;
            _heartBeatHandler = new HeartBeatHandler(
                SendHeartBeat,
                Close,
                (int)parameters.Heartbeat.TotalSeconds);
            IsClosed = false;
        }

        public delegate Task ConnectionCloseHandler(string reason);

        public event ConnectionCloseHandler ConnectionClosed;

        private async Task OnConnectionClosed(string reason)
        {
            if (ConnectionClosed != null)
            {
                await ConnectionClosed?.Invoke(reason)!;
            }
        }

        public static async Task<Client> Create(ClientParameters parameters)
        {
            var client = new Client(parameters);

            client.connection = await Connection.Create(parameters.Endpoint,
                client.HandleIncoming, client.HandleClosed, parameters.Ssl);

            // exchange properties
            var peerPropertiesResponse =
                await client.Request<PeerPropertiesRequest, PeerPropertiesResponse>(corr =>
                    new PeerPropertiesRequest(corr, parameters.Properties));
            foreach (var (k, v) in peerPropertiesResponse.Properties)
            {
                Debug.WriteLine($"server Props {k} {v}");
            }

            //auth
            var saslHandshakeResponse =
                await client.Request<SaslHandshakeRequest, SaslHandshakeResponse>(
                    corr => new SaslHandshakeRequest(corr));
            foreach (var m in saslHandshakeResponse.Mechanisms)
            {
                Debug.WriteLine($"sasl mechanism: {m}");
            }

            var saslData = Encoding.UTF8.GetBytes($"\0{parameters.UserName}\0{parameters.Password}");
            var authResponse =
                await client.Request<SaslAuthenticateRequest, SaslAuthenticateResponse>(corr =>
                    new SaslAuthenticateRequest(corr, "PLAIN", saslData));
            Debug.WriteLine($"auth: {authResponse.ResponseCode} {authResponse.Data}");
            ClientExceptions.MaybeThrowException(authResponse.ResponseCode, parameters.UserName);

            //tune
            var tune = await client.tuneReceived.Task;
            await client.Publish(new TuneRequest(0,
                (uint)client.Parameters.Heartbeat.TotalSeconds));

            // open 
            var open = await client.Request<OpenRequest, OpenResponse>(corr =>
                new OpenRequest(corr, parameters.VirtualHost));
            ClientExceptions.MaybeThrowException(open.ResponseCode, parameters.VirtualHost);

            Debug.WriteLine($"open: {open.ResponseCode} {open.ConnectionProperties.Count}");
            foreach (var (k, v) in open.ConnectionProperties)
            {
                Debug.WriteLine($"open prop: {k} {v}");
            }

            client.ConnectionProperties = open.ConnectionProperties;

            client.correlationId = 100;
            return client;
        }

        public async ValueTask<bool> Publish(Publish publishMsg)
        {
            var publishTask = Publish<Publish>(publishMsg);
            if (!publishTask.IsCompletedSuccessfully)
            {
                await publishTask.ConfigureAwait(false);
            }

            publishCommandsSent += 1;
            messagesSent += publishMsg.MessageCount;
            return publishTask.Result;
        }

        public ValueTask<bool> Publish<T>(T msg) where T : struct, ICommand
        {
            return connection.Write(msg);
        }

        public async Task<(byte, DeclarePublisherResponse)> DeclarePublisher(string publisherRef,
            string stream,
            Action<ReadOnlyMemory<ulong>> confirmCallback,
            Action<(ulong, ResponseCode)[]> errorCallback)
        {
            var publisherId = nextPublisherId++;
            publishers.Add(publisherId, (confirmCallback, errorCallback));
            return (publisherId, await Request<DeclarePublisherRequest, DeclarePublisherResponse>(corr =>
                new DeclarePublisherRequest(corr, publisherId, publisherRef, stream)));
        }

        public async Task<DeletePublisherResponse> DeletePublisher(byte publisherId)
        {
            try
            {
                var result =
                    await Request<DeletePublisherRequest, DeletePublisherResponse>(corr =>
                        new DeletePublisherRequest(corr, publisherId));

                return result;
            }
            finally
            {
                publishers.Remove(publisherId);
            }
        }

        public async Task<(byte, SubscribeResponse)> Subscribe(string stream, IOffsetType offsetType,
            ushort initialCredit,
            Dictionary<string, string> properties, Func<Deliver, Task> deliverHandler)
        {
            var subscriptionId = GetNextSubscriptionId();
            consumers.Add(subscriptionId, deliverHandler);
            return (subscriptionId,
                await Request<SubscribeRequest, SubscribeResponse>(corr =>
                    new SubscribeRequest(corr, subscriptionId, stream, offsetType, initialCredit, properties)));
        }

        public async Task<UnsubscribeResponse> Unsubscribe(byte subscriptionId)
        {
            try
            {
                var result =
                    await Request<UnsubscribeRequest, UnsubscribeResponse>(corr =>
                        new UnsubscribeRequest(corr, subscriptionId));
                return result;
            }
            finally
            {
                // remove consumer after RPC returns, this should avoid uncorrelated data being sent
                consumers.Remove(subscriptionId);
            }
        }

        private async ValueTask<TOut> Request<TIn, TOut>(Func<uint, TIn> request, TimeSpan? timeout = null)
            where TIn : struct, ICommand where TOut : struct, ICommand
        {
            var corr = NextCorrelationId();
            var tcs = PooledTaskSource<TOut>.Rent();
            requests.TryAdd(corr, tcs);
            await Publish(request(corr));
            using var cts = new CancellationTokenSource(timeout ?? defaultTimeout);
            await using (cts.Token.Register(
                             valueTaskSource =>
                                 ((ManualResetValueTaskSource<TOut>)valueTaskSource).SetException(
                                     new TimeoutException()), tcs))
            {
                var valueTask = new ValueTask<TOut>(tcs, tcs.Version);
                var result = await valueTask;
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
            await OnConnectionClosed(reason);
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
                    var (confirmCallback, _) = publishers[confirm.PublisherId];
                    confirmCallback(confirm.PublishingIds);
                    if (MemoryMarshal.TryGetArray(confirm.PublishingIds, out var confirmSegment))
                    {
                        ArrayPool<ulong>.Shared.Return(confirmSegment.Array);
                    }

                    break;
                case Deliver.Key:
                    Deliver.Read(frame, out var deliver);
                    var deliverHandler = consumers[deliver.SubscriptionId];
                    await deliverHandler(deliver).ConfigureAwait(false);
                    break;
                case PublishError.Key:
                    PublishError.Read(frame, out var error);
                    var (_, errorCallback) = publishers[error.PublisherId];
                    errorCallback(error.PublishingErrors);
                    break;
                case MetaDataUpdate.Key:
                    MetaDataUpdate.Read(frame, out var metaDataUpdate);
                    Parameters.MetadataHandler(metaDataUpdate);
                    break;
                case TuneResponse.Key:
                    TuneResponse.Read(frame, out var tuneResponse);
                    tuneReceived.SetResult(tuneResponse);
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
                default:
                    if (MemoryMarshal.TryGetArray(frame.First, out var segment))
                    {
                        ArrayPool<byte>.Shared.Return(segment.Array);
                    }

                    throw new ArgumentException($"Unknown or unexpected tag: {tag}", nameof(tag));
            }
        }

        private void HandleCorrelatedResponse<T>(T command) where T : struct, ICommand
        {
            if (command.CorrelationId == uint.MaxValue)
            {
                throw new Exception($"unhandled incoming command {command.GetType()}");
            }

            if (requests.TryRemove(command.CorrelationId, out var tsc))
            {
                ((ManualResetValueTaskSource<T>)tsc).SetResult(command);
            }
        }

        private async ValueTask<bool> SendHeartBeat()
        {
            return await Publish(new HeartBeatRequest());
        }

        private void InternalClose()
        {
            _heartBeatHandler.Close();
            IsClosed = true;
        }

        public async Task<CloseResponse> Close(string reason)
        {
            if (IsClosed)
            {
                return new CloseResponse(0, ResponseCode.Ok);
            }

            // TODO LRB timeout
            var result =
                await Request<CloseRequest, CloseResponse>(corr => new CloseRequest(corr, reason),
                    TimeSpan.FromSeconds(30));
            try
            {
                InternalClose();
                connection.Dispose();
            }
            catch (Exception e)
            {
                LogEventSource.Log.LogError($"An error occurred while calling {nameof(connection.Dispose)}.", e);
            }

            return result;
        }

        // Safe close 
        // the client can be closed only if the publishers are == 0
        // not a public method used internally by producers and consumers
        internal CloseResponse MaybeClose(string reason)
        {
            if (publishers.Count == 0 && consumers.Count == 0)
            {
                return Close(reason).Result;
            }

            var result = new CloseResponse(0, ResponseCode.Ok);
            return result;
        }

        public async ValueTask<QueryPublisherResponse> QueryPublisherSequence(string publisherRef, string stream)
        {
            return await Request<QueryPublisherRequest, QueryPublisherResponse>(corr =>
                new QueryPublisherRequest(corr, publisherRef, stream));
        }

        public async ValueTask<bool> StoreOffset(string reference, string stream, ulong offsetValue)
        {
            return await Publish(new StoreOffsetRequest(stream, reference, offsetValue));
        }

        public async ValueTask<MetaDataResponse> QueryMetadata(string[] streams)
        {
            return await Request<MetaDataQuery, MetaDataResponse>(corr => new MetaDataQuery(corr, streams.ToList()));
        }

        public async ValueTask<QueryOffsetResponse> QueryOffset(string reference, string stream)
        {
            return await Request<QueryOffsetRequest, QueryOffsetResponse>(corr =>
                new QueryOffsetRequest(stream, corr, reference));
        }

        public async ValueTask<CreateResponse> CreateStream(string stream, IDictionary<string, string> args)
        {
            return await Request<CreateRequest, CreateResponse>(corr => new CreateRequest(corr, stream, args));
        }

        public async ValueTask<DeleteResponse> DeleteStream(string stream)
        {
            return await Request<DeleteRequest, DeleteResponse>(corr => new DeleteRequest(corr, stream));
        }

        public async ValueTask<bool> Credit(byte subscriptionId, ushort credit)
        {
            return await Publish(new CreditRequest(subscriptionId, credit));
        }
    }

    public static class PooledTaskSource<T>
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

    public sealed class ManualResetValueTaskSource<T> : IValueTaskSource<T>, IValueTaskSource
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
        public void SetException(Exception error) => _logic.SetException(error);

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
