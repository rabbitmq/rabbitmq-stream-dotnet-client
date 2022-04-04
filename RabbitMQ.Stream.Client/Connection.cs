// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public class Connection : IDisposable
    {
        private readonly Socket socket;
        private readonly PipeWriter writer;
        private readonly PipeReader reader;
        private Func<Memory<byte>, Task> commandCallback;
        private readonly Func<string, Task> closedCallback;
        private int numFrames;
        private readonly object writeLock = new();
        internal int NumFrames => numFrames;
        private bool isClosed = false;

        public bool IsClosed => isClosed;

        internal Func<Memory<byte>, Task> CommandCallback
        {
            get => commandCallback;
            set => commandCallback = value;
        }

        private static System.IO.Stream MaybeTcpUpgrade(NetworkStream networkStream, SslOption sslOption)
        {
            return sslOption is { Enabled: false } ? networkStream : SslHelper.TcpUpgrade(networkStream, sslOption);
        }

        private Connection(Socket socket, Func<Memory<byte>, Task> callback,
            Func<string, Task> closedCallBack, SslOption sslOption)
        {
            this.socket = socket;

            commandCallback = callback;
            closedCallback = closedCallBack;
            var networkStream = new NetworkStream(socket);
            var stream = MaybeTcpUpgrade(networkStream, sslOption);
            writer = PipeWriter.Create(stream);
            reader = PipeReader.Create(stream);
            // TODO LRB this task needs to be shut down
            Task.Run(ProcessIncomingFrames);
        }

        public static async Task<Connection> Create(EndPoint ipEndpoint, Func<Memory<byte>, Task> commandCallback,
            Func<string, Task> closedCallBack, SslOption sslOption)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            //TODO: make configurable
            socket.SendBufferSize *= 10;
            socket.ReceiveBufferSize *= 10;

            await socket.ConnectAsync(ipEndpoint);
            return new Connection(socket, commandCallback, closedCallBack, sslOption);
        }

        private ValueTask<FlushResult> FlushAsync()
        {
            lock (writeLock)
            {
                var flushTask = writer.FlushAsync();
                return flushTask;
            }
        }

        public async ValueTask<bool> Write<T>(T command) where T : struct, ICommand
        {
            WriteCommand(command);
            var flushTask = FlushAsync();

            // Let's check if this is completed synchronously before invoking the async state machine
            if (!flushTask.IsCompletedSuccessfully)
            {
                await flushTask.ConfigureAwait(false);
            }

            return flushTask.Result.IsCompleted;
        }

        private void WriteCommand<T>(T command) where T : struct, ICommand
        {
            // Only one thread should be able to write to the output pipeline at a time.
            lock (writeLock)
            {
                var size = command.SizeNeeded;
                var mem = writer.GetSpan(4 + size); // + 4 to write the size
                WireFormatting.WriteUInt32(mem, (uint)size);
                var written = command.Write(mem.Slice(4));
                Debug.Assert(size == written);
                writer.Advance(4 + written);
            }
        }

        private async Task ProcessIncomingFrames()
        {
            try
            {
                while (true)
                {
                    if (!reader.TryRead(out var result))
                    {
                        result = await reader.ReadAsync().ConfigureAwait(false);
                    }

                    var buffer = result.Buffer;
                    if (buffer.Length == 0)
                    {
                        Debug.WriteLine("TCP Connection Closed");
                        // We're not going to receive any more bytes from the connection.
                        break;
                    }

                    // Let's try to read some frames!

                    while (TryReadFrame(ref buffer, out var frame))
                    {
                        // Let's rent some memory to copy the frame from the network stream. This memory will be reclaimed once the frame has been handled.

                        // Console.WriteLine(
                        //     $"B TryReadFrame {buffer.Length} {result.IsCompleted} {result.Buffer.IsEmpty} {frame.Length}");

                        var memory =
                            ArrayPool<byte>.Shared.Rent((int)frame.Length).AsMemory(0, (int)frame.Length);
                        frame.CopyTo(memory.Span);
                        await commandCallback(memory).ConfigureAwait(false);
                        numFrames += 1;
                    }

                    reader.AdvanceTo(buffer.Start, buffer.End);
                }

                // Mark the PipeReader as complete

                await reader.CompleteAsync();
            }
            catch (Exception e)
            {
                // The exception is needed mostly to raise the 
                // closedCallback event.
                // It is useful to trace the error, but at this point
                // the socket is closed maybe not in the correct way
                Debug.WriteLine($"Error reading the socket, error: {e}");
            }
            finally
            {
                isClosed = true;
                await closedCallback?.Invoke("TCP Connection Closed")!;
                Debug.WriteLine("TCP Connection Closed");
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
            writer.Complete();
            reader.Complete();
            socket.Dispose();
        }
    }
}
