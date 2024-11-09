// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client
{
    internal static class ConnectionClosedReason
    {
        public const string Normal = "TCP connection closed normal";
        public const string Unexpected = "TCP connection closed unexpected";
        public const string TooManyHeartbeatsMissing = "TCP connection closed by too many heartbeats missing";

    }

    public class Connection : IDisposable
    {
        private readonly Socket _socket;
        private readonly PipeWriter _writer;
        private readonly PipeReader _reader;
        private readonly Task _incomingFramesTask;
        private readonly Func<Memory<byte>, Task> _commandCallback;
        private readonly Func<string, Task> _closedCallback;
        private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);
        private int numFrames;
        private bool isClosed = false;
        private string _closedReason = ConnectionClosedReason.Unexpected;
        private bool _disposedValue;
        private readonly ILogger _logger;

        // this is used to cancel the socket and the reader/write operations tasks
        private readonly CancellationTokenSource _cancelTokenSource = new();
        private CancellationToken Token => _cancelTokenSource.Token;

        internal int NumFrames => numFrames;
        internal string ClientId { get; set; }
        public bool IsClosed => isClosed;
        public void UpdateCloseStatus(string reason)
        {
            _closedReason = reason;
        }

        private static System.IO.Stream MaybeTcpUpgrade(NetworkStream networkStream, SslOption sslOption)
        {
            return sslOption is { Enabled: false } ? networkStream : SslHelper.TcpUpgrade(networkStream, sslOption);
        }

        private Connection(Socket socket, Func<Memory<byte>, Task> callback,
            Func<string, Task> closedCallBack, SslOption sslOption, ILogger logger)
        {
            _logger = logger;
            _socket = socket;
            _commandCallback = callback;
            _closedCallback = closedCallBack;
            var networkStream = new NetworkStream(socket);
            var stream = MaybeTcpUpgrade(networkStream, sslOption);
            _writer = PipeWriter.Create(stream);
            _reader = PipeReader.Create(stream);
            // ProcessIncomingFrames is dropped as soon as the connection is closed
            // no need to stop it manually when the connection is closed
            _incomingFramesTask = Task.Run(ProcessIncomingFrames);
        }

        public static async Task<Connection> Create(EndPoint endpoint, Func<Memory<byte>, Task> commandCallback,
            Func<string, Task> closedCallBack, SslOption sslOption, ILogger logger)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            //TODO: make configurable
            socket.SendBufferSize *= 10;
            socket.ReceiveBufferSize *= 10;

            try
            {
                await socket.ConnectAsync(endpoint).ConfigureAwait(false);
            }
            catch (SocketException ex)
            {
                switch (endpoint)
                {
                    case IPEndPoint ipEndPoint:
                        ex.Data.Add("Address", ipEndPoint.Address);
                        ex.Data.Add("Port", ipEndPoint.Port);
                        throw;
                    case DnsEndPoint dnsEndPoint:
                        ex.Data.Add("Host", dnsEndPoint.Host);
                        ex.Data.Add("Port", dnsEndPoint.Port);
                        throw;
                    default:
                        throw;
                }
            }

            return new Connection(socket, commandCallback, closedCallBack, sslOption, logger);
        }

        public ValueTask<bool> Write<T>(T command) where T : struct, ICommand
        {
            if (!_writeLock.Wait(0))
            {
                // https://blog.marcgravell.com/2018/07/pipe-dreams-part-3.html
                var writeSlowPath = WriteCommandAsyncSlowPath(command);
                writeSlowPath.ConfigureAwait(false);
                return writeSlowPath;
            }
            else
            {
                var release = true;
                try
                {
                    var payloadSize = WriteCommandPayloadSize(command);
                    var written = command.Write(_writer);
                    Debug.Assert(payloadSize == written);
                    var flush = _writer.FlushAsync();
                    flush.ConfigureAwait(false);
                    if (flush.IsCompletedSuccessfully)
                    {
                        // we return true to indicate that the command was written
                        // In this PR https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/220
                        // we made all WriteCommand async so await is enough to indicate that the command was written
                        // We decided to keep the return value to avoid a breaking change
                        return ValueTask.FromResult(true);
                    }
                    else
                    {
                        release = false;
                        return AwaitFlushThenRelease(flush);
                    }
                }
                finally
                {
                    if (release)
                    {
                        _writeLock.Release();
                    }
                }
            }
        }

        private async ValueTask<bool> WriteCommandAsyncSlowPath<T>(T command) where T : struct, ICommand
        {
            if (Token.IsCancellationRequested)
            {
                throw new OperationCanceledException("Token Cancellation Requested Connection");
            }

            if (isClosed)
            {
                throw new InvalidOperationException("Connection is closed");
            }

            // Only one thread should be able to write to the output pipeline at a time.
            await _writeLock.WaitAsync(Token).ConfigureAwait(false);
            try
            {
                var payloadSize = WriteCommandPayloadSize(command);
                var written = command.Write(_writer);
                Debug.Assert(payloadSize == written);
                await _writer.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }

            return true;
        }

        private async ValueTask<bool> AwaitFlushThenRelease(ValueTask<FlushResult> task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }

            return true;
        }

        private int WriteCommandPayloadSize<T>(T command) where T : struct, ICommand
        {
            /*
             * TODO FUTURE
             * This code could be moved into a common base class for all outgoing
             * commands
             */
            var payloadSize = command.SizeNeeded;
            var span = _writer.GetSpan(WireFormatting.SizeofUInt32); // 4 to write the size
            WireFormatting.WriteUInt32(span, (uint)payloadSize);
            _writer.Advance(WireFormatting.SizeofUInt32);
            return payloadSize;
        }

        private async Task ProcessIncomingFrames()
        {
            Exception caught = null;
            try
            {
                while (!isClosed)
                {
                    if (!_reader.TryRead(out var result))
                    {
                        result = await _reader.ReadAsync(Token).ConfigureAwait(false);
                    }

                    var buffer = result.Buffer;
                    if (buffer.Length == 0)
                    {
                        Debug.WriteLine("TCP Connection Closed!");
                        // We're not going to receive any more bytes from the connection.
                        break;
                    }

                    // Let's try to read some frames!

                    while (TryReadFrame(ref buffer, out var frame) && !isClosed)
                    {
                        // Let's rent some memory to copy the frame from the network stream. This memory will be reclaimed once the frame has been handled.
                        var memory =
                            ArrayPool<byte>.Shared.Rent((int)frame.Length).AsMemory(0, (int)frame.Length);
                        frame.CopyTo(memory.Span);

                        await _commandCallback(memory).ConfigureAwait(false);
                        numFrames += 1;
                    }

                    _reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            catch (OperationCanceledException e)
            {
                caught = e;
                // We don't need to log as error this exception
                // It is raised when the socket is closed due of cancellation token 
                // from the producer or consumer class
                // we leave it as debug to avoid noise in the logs
                _logger?.LogDebug("Operation Canceled Exception. TCP Connection Closed");
            }
            catch (Exception e)
            {
                caught = e;
                // The exception is needed mostly to raise the 
                // closedCallback event.
                // It is useful to trace the error, but at this point
                // the socket is closed maybe not in the correct way
                if (!isClosed)
                {
                    _logger?.LogError(e, "Error reading the socket");
                }
            }
            finally
            {
                isClosed = true;
                _logger?.LogDebug(
                    "TCP Connection Closed ClientId: {ClientId}, Reason {Reason}. IsCancellationRequested {Token} ",
                    ClientId, _closedReason, Token.IsCancellationRequested);
                // Mark the PipeReader as complete
                await _reader.CompleteAsync(caught).ConfigureAwait(false);
                _closedCallback?.Invoke(_closedReason)!.ConfigureAwait(false);
            }
        }

        private static bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> frame)
        {
            // Do we have enough bytes in the buffer to begin parsing a frame?

            if (buffer.Length > 4)
            {
                // Let's see how big the next frame is
                WireFormatting.ReadUInt32(buffer, out var frameSize);
                if (buffer.Length >= 4 + frameSize)
                {
                    // We have enough bytes in the buffer to read a whole frame so let's read it
                    frame = buffer.Slice(4, frameSize);

                    // Let's slice the buffer at the end of the current frame
                    buffer = buffer.Slice(frame.End);
                    return true;
                }
            }

            frame = ReadOnlySequence<byte>.Empty;
            return false;
        }

        public void Dispose()
        {
            if (!_disposedValue)
            {
                try
                {
                    if (!_cancelTokenSource.IsCancellationRequested)
                    {
                        _cancelTokenSource.Cancel();
                    }

                    isClosed = true;
                    _writer.Complete();
                    _reader.Complete();
                    _socket.Close();
                    if (!_incomingFramesTask.Wait(Consts.MidWait))
                    {
                        _logger?.LogWarning("ProcessIncomingFrames reader task did not exit in {MidWait}",
                            Consts.MidWait);
                    }
                }
                finally
                {
                    _disposedValue = true;
                }
            }

            GC.SuppressFinalize(this);
        }
    }
}
