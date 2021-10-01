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

        public async Task<bool> Write(ICommand command)
        {
            var size = command.SizeNeeded;
            var mem = writer.GetMemory(size + 4); // + 4 to write the size
            WireFormatting.WriteUInt32(mem.Span, (uint) size);
            var written = command.Write(mem.Span.Slice(4));
            Debug.Assert(size == written);
            writer.Advance(written + 4);
            var result = await writer.FlushAsync();
            return result.IsCompleted;
        }
        
        private async Task ProcessIncomingFrames()
        {
            while (true)
            {
                var result = await reader.ReadAsync();

                var buffer = result.Buffer;
                var offset = WireFormatting.ReadUInt32(buffer, out var length);
                if(buffer.Length >= length + 4)
                {
                    // there is enough data in the buffer to process the a frame
                    var frame = buffer.Slice(offset, length);
                    WireFormatting.ReadUInt16(buffer.Slice(offset, 2), out var tag);
                    var isResponse = (tag & 0x8000) != 0;
                    if (isResponse)
                    {
                        tag = (ushort)(tag ^ 0x8000);
                    }
                    offset += HandleFrame(tag, frame);
                    //advance the stream reader
                    reader.AdvanceTo(frame.End, frame.End);
                }
                else
                {
                    // mark stuff as read but not consumed
                    // TODO work out if there is an off by one issue here
                    reader.AdvanceTo(buffer.Start, buffer.End);
                }

                // Stop reading if there's no more data coming.
                if (!result.IsCompleted) continue;
                break;
            }

            // Mark the PipeReader as complete.
            await reader.CompleteAsync();
        }

        private int HandleFrame(ushort tag, ReadOnlySequence<byte> frame)
        {
            var offset = 0;
            ICommand command;
            switch (tag)
            {
                case PeerPropertiesResponse.Key:
                    offset = PeerPropertiesResponse.Read(frame, out command);
                    commandCallback(command);
                    break;
                case SaslHandshakeResponse.Key:
                    offset = SaslHandshakeResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case SaslAuthenticateResponse.Key:
                    offset = SaslAuthenticateResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case TuneResponse.Key:
                    offset = TuneResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case OpenResponse.Key:
                    offset = OpenResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case DeclarePublisherResponse.Key:
                    offset = DeclarePublisherResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case DeletePublisherResponse.Key:
                    offset = DeletePublisherResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case QueryPublisherResponse.Key:
                    offset =  QueryPublisherResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case PublishConfirm.Key:
                    offset = PublishConfirm.Read(frame, out command);
                     commandCallback(command);
                    break;
                case PublishError.Key:
                    offset = PublishError.Read(frame, out command);
                     commandCallback(command);
                    break;
                case SubscribeResponse.Key:
                    offset = SubscribeResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case UnsubscribeResponse.Key:
                    offset = UnsubscribeResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case Deliver.Key:
                    offset = Deliver.Read(frame, out command);
                     commandCallback(command);
                    break;
                case CloseResponse.Key:
                    offset = CloseResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case CreateResponse.Key:
                    offset = CreateResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case DeleteResponse.Key:
                    offset = DeleteResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case MetaDataResponse.Key:
                    offset = MetaDataResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
                case MetaDataUpdate.Key:
                    offset = MetaDataUpdate.Read(frame, out command);
                     commandCallback(command);
                    break;
                case QueryOffsetResponse.Key:
                    offset = QueryOffsetResponse.Read(frame, out command);
                     commandCallback(command);
                    break;
            }
            
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
