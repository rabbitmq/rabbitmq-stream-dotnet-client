// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    // These tests don't need a broker.
    // The client is connected to a local socket that swallows the requests,
    // and the server frames are pushed directly to Client.HandleIncoming.
    public class ClientRequestTests : IAsyncLifetime
    {
        private TcpListener _listener;
        private TcpClient _serverSide;
        private Client _client;
        private Task _serverSideClosed;

        public async Task InitializeAsync()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            var accept = _listener.AcceptTcpClientAsync();
            _client = await Client.CreateWithoutHandshake(new ClientParameters
            {
                Endpoint = _listener.LocalEndpoint,
                RpcTimeOut = TimeSpan.FromMilliseconds(200)
            });
            _serverSide = await accept;
            // completes when the client closes the socket
            _serverSideClosed = DrainAsync(_serverSide.GetStream());
        }

        public Task DisposeAsync()
        {
            _serverSide?.Dispose();
            _listener?.Stop();
            return Task.CompletedTask;
        }

        private static async Task DrainAsync(NetworkStream stream)
        {
            var buffer = new byte[4096];
            try
            {
                while (await stream.ReadAsync(buffer) > 0)
                {
                }
            }
            catch (Exception)
            {
                // the socket is closed at the end of the test
            }
        }

        // HandleIncoming returns the frame to the ArrayPool, so the frame must be rented from it
        private static Memory<byte> RentFrame(int size)
        {
            return ArrayPool<byte>.Shared.Rent(size).AsMemory(0, size);
        }

        // MetaDataResponse with no brokers and no streams
        private static Memory<byte> MetaDataResponseFrame(uint correlationId)
        {
            var frame = RentFrame(16);
            var span = frame.Span;
            var offset = WireFormatting.WriteUInt16(span, MetaDataResponse.Key);
            offset += WireFormatting.WriteUInt16(span[offset..], 1);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            offset += WireFormatting.WriteUInt32(span[offset..], 0);
            WireFormatting.WriteUInt32(span[offset..], 0);
            return frame;
        }

        private static Memory<byte> ConsumerUpdateQueryFrame(uint correlationId, byte subscriptionId)
        {
            var frame = RentFrame(10);
            var span = frame.Span;
            var offset = WireFormatting.WriteUInt16(span, ConsumerUpdateQueryResponse.Key);
            offset += WireFormatting.WriteUInt16(span[offset..], 1);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            span[offset] = subscriptionId;
            span[offset + 1] = 1;
            return frame;
        }

        private ValueTask<MetaDataResponse> QueryMetadata(TimeSpan timeout, Action<uint> onCorrelationId)
        {
            return _client.Request<MetaDataQuery, MetaDataResponse>(corr =>
            {
                onCorrelationId(corr);
                return new MetaDataQuery(corr, new[] { "stream" });
            }, timeout);
        }

        [Fact]
        public async Task LateResponseAfterTimeoutMustBeIgnored()
        {
            // The server answers after the request is timed out.
            // The late response must not fail the socket reader with
            // InvalidOperationException from ManualResetValueTaskSourceCore.SetResult
            uint timedOutCorrelationId = 0;
            await Assert.ThrowsAsync<TimeoutException>(async () =>
                await QueryMetadata(TimeSpan.FromMilliseconds(100), corr => timedOutCorrelationId = corr));

            var error = await Record.ExceptionAsync(() =>
                _client.HandleIncoming(MetaDataResponseFrame(timedOutCorrelationId)));
            Assert.Null(error);

            // the client must still be able to complete the next requests
            uint correlationId = 0;
            var request = QueryMetadata(TimeSpan.FromSeconds(5), corr => correlationId = corr);
            await _client.HandleIncoming(MetaDataResponseFrame(correlationId));
            var response = await request;
            Assert.Equal(correlationId, response.CorrelationId);
        }

        [Fact]
        public async Task ConsumerUpdateQueryMustNotCompleteClientRequests()
        {
            // ConsumerUpdateQuery is sent by the server with its own correlation sequence.
            // It must not complete a pending client request with the same correlation id.
            // The subscription does not exist: the client must answer without failing.
            uint correlationId = 0;
            var request = QueryMetadata(TimeSpan.FromSeconds(5), corr => correlationId = corr);

            var error = await Record.ExceptionAsync(() =>
                _client.HandleIncoming(ConsumerUpdateQueryFrame(correlationId, 99)));
            Assert.Null(error);

            await _client.HandleIncoming(MetaDataResponseFrame(correlationId));
            var response = await request;
            Assert.Equal(correlationId, response.CorrelationId);
        }
        [Fact]
        public async Task SubscribeTimeoutMustNotLeaveAZombieSubscription()
        {
            // The server does not answer the subscribe (e.g. cluster under stress) but it could
            // have registered the subscription. The client can't remove it (the unsubscribe
            // times out too), so it must close the socket: leaving the connection open keeps
            // the subscription alive on the server, and with single active consumer the server
            // can promote it as active while nobody processes the messages.
            await Assert.ThrowsAsync<TimeoutException>(() =>
                _client.Subscribe(new RawConsumerConfig("stream"), 10,
                    new System.Collections.Generic.Dictionary<string, string>(),
                    _ => Task.CompletedTask,
                    _ => Task.FromResult<IOffsetType>(new OffsetTypeNext())));

            Assert.Empty(_client.Consumers);
            // the socket is closed also when the close request times out
            var closed = await Task.WhenAny(_serverSideClosed, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.Same(_serverSideClosed, closed);
            Assert.True(_client.IsClosed);
        }
    }
}
