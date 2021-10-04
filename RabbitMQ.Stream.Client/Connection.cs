using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public class Connection : IDisposable
    {
        private readonly Socket socket;
        private readonly PipeWriter writer;
        private readonly PipeReader reader;
        private readonly Task readerTask;
        private Func<ICommand, Task> commandCallback;

        private int numFrames;

        internal int NumFrames => numFrames;

        internal Func<ICommand, Task> CommandCallback
        {
            get => commandCallback;
            set => commandCallback = value;
        }

        private Connection(Socket socket, Func<ICommand, Task> callback)
        {
            this.socket = socket;
            this.commandCallback = callback;
            var stream = new NetworkStream(socket);
            writer = PipeWriter.Create(stream);
            reader = PipeReader.Create(stream);
            var ts = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);
            readerTask = ts.StartNew(ProcessIncomingFrames);
        }

        public static async Task<Connection> Create(EndPoint ipEndpoint, Func<ICommand, Task> commandCallback)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            //TODO: make configurable
            socket.SendBufferSize *= 10;
            socket.ReceiveBufferSize *= 10;
            await socket.ConnectAsync(ipEndpoint);
            return new Connection(socket, commandCallback);
        }

        public async ValueTask<bool> Write(ICommand command)
        {
            writer.Advance(WriteCommand(command));
            var flushTask = writer.FlushAsync();

            // Let's check if this completed synchronously befor invoking the async state mahcine
            if (!flushTask.IsCompletedSuccessfully)
            {
                await flushTask.ConfigureAwait(false);
            }

            return flushTask.Result.IsCompleted;
        }

        private int WriteCommand(ICommand command)
        {
            var size = command.SizeNeeded;
            var mem = writer.GetSpan(4 + size); // + 4 to write the size
            WireFormatting.WriteUInt32(mem, (uint)size);
            var written = command.Write(mem.Slice(4));
            Debug.Assert(size == written);
            return 4 + written;
        }

        private async Task ProcessIncomingFrames()
        {
            while (true)
            {
                var readerTask = reader.ReadAsync();

                // Let's check if this completed synchronously befor invoking the async state mahcine
                if (!readerTask.IsCompletedSuccessfully)
                {
                    await readerTask.ConfigureAwait(false);
                }

                var result = readerTask.Result;
                var buffer = result.Buffer;
                if (buffer.Length == 0)
                {
                    // We're not going to receive any more bytes from the connection.
                    break;
                }

                // Let's try to read some frames!
                while(TryReadFrame(ref buffer, out ReadOnlySequence<byte> frame))
                {
                    // Let's check the frame tag
                    WireFormatting.ReadUInt16(frame, out var tag);
                    if ((tag & 0x8000) != 0)
                    {
                        tag = (ushort)(tag ^ 0x8000);
                    }

                    // Let's handle the frame
                    HandleFrame(tag, frame);
                }

                reader.AdvanceTo(buffer.Start, buffer.End);
            }

            // Mark the PipeReader as complete.
            await reader.CompleteAsync();
        }

        private bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> frame)
        {
            // Do we have enough bytes in the buffer to begin parsing a frame?
            if (buffer.Length > 4)
            {
                // Let's see how big the next frame is
                WireFormatting.ReadUInt32(buffer, out var frameSize);
                if (buffer.Length >= 4 + frameSize)
                {
                    // We have enough bytes in the buffer to read a whole frame

                    // Read the frame data
                    frame = buffer.Slice(4, frameSize);

                    // Let's slice the buffer
                    buffer = buffer.Slice(4 + frameSize);
                    return true;
                }
            }

            frame = ReadOnlySequence<byte>.Empty;
            return false;
        }

        private int HandleFrame(ushort tag, ReadOnlySequence<byte> frame)
        {
            ICommand command;
            int offset = tag switch
            {
                DeclarePublisherResponse.Key => DeclarePublisherResponse.Read(frame, out command),
                PublishConfirm.Key => PublishConfirm.Read(frame, out command),
                PublishError.Key => PublishError.Read(frame, out command),
                QueryPublisherResponse.Key => QueryPublisherResponse.Read(frame, out command),
                DeletePublisherResponse.Key => DeletePublisherResponse.Read(frame, out command),
                SubscribeResponse.Key => SubscribeResponse.Read(frame, out command),
                Deliver.Key => Deliver.Read(frame, out command),
                QueryOffsetResponse.Key => QueryOffsetResponse.Read(frame, out command),
                UnsubscribeResponse.Key => UnsubscribeResponse.Read(frame, out command),
                CreateResponse.Key => CreateResponse.Read(frame, out command),
                DeleteResponse.Key => DeleteResponse.Read(frame, out command),
                MetaDataResponse.Key => MetaDataResponse.Read(frame, out command),
                MetaDataUpdate.Key => MetaDataUpdate.Read(frame, out command),
                PeerPropertiesResponse.Key => PeerPropertiesResponse.Read(frame, out command),
                SaslHandshakeResponse.Key => SaslHandshakeResponse.Read(frame, out command),
                SaslAuthenticateResponse.Key => SaslAuthenticateResponse.Read(frame, out command),
                TuneResponse.Key => TuneResponse.Read(frame, out command),
                OpenResponse.Key => OpenResponse.Read(frame, out command),
                CloseResponse.Key => CloseResponse.Read(frame, out command),
                _ => throw new ArgumentException($"Unknown or unexpected tag: {tag}", nameof(tag))
            };

            commandCallback(command);

            this.numFrames += 1;
            return offset;
        }

        public void Dispose()
        {
            writer.Complete();
            reader.Complete();
            socket.Dispose();
        }
    }
}
