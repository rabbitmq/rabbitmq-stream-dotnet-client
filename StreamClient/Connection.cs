using System;
using System.Buffers;
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
        private readonly Task readerTask;
        private Action<ICommand> commandCallback;

        public Connection(Action<ICommand> callback)
        {
            this.commandCallback = callback;
            socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(new IPEndPoint(IPAddress.Loopback, 5552));
            var stream = new NetworkStream(socket);
            writer = PipeWriter.Create(stream);
            reader = PipeReader.Create(stream);
            readerTask = Task.Run(() => ProcessIncomingFrames(stream, callback));
        }

        public async Task<bool> Write(ICommand command)
        {
            var size = command.SizeNeeded;
            // Console.WriteLine($"need memory {size + 4}");
            var mem = writer.GetMemory(size + 4); // + 4 to write the size
            // Console.WriteLine($"got memory {mem.Length}");
            WireFormatting.WriteUInt32(mem.Span, (uint) size);
            var written = command.Write(mem.Span.Slice(4));
            writer.Advance(written + 4);
            var result = await writer.FlushAsync();
            return result.IsCompleted;
        }
        private async Task ProcessIncomingFrames(NetworkStream stream, Action<ICommand> commandCallback)
        {
            while (true)
            {
                ReadResult result = await reader.ReadAsync();

                if(result.IsCompleted)
                {
                    Console.WriteLine($"return ");
                    return;
                }
                ReadOnlySequence<byte> buffer = result.Buffer;
                UInt32 length;
                var offset = WireFormatting.ReadUInt32(buffer, out length);
                if(buffer.Length >= length + 4)
                {
                    // there is enough data in the buffer to process the a frame
                    var frame = buffer.Slice(offset, length);
                    ushort tag;
                    WireFormatting.ReadUInt16(buffer.Slice(offset, 2), out tag);
                    var isResponse = (tag & 0x8000) != 0;
                    // Console.WriteLine($"tag [{tag}]{tag ^ 0x8000} {tag & 0x8000} {isResponse}");
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
                if (result.IsCompleted)
                {
                    break;
                }
            }

            // Mark the PipeReader as complete.
            await reader.CompleteAsync();
            Console.WriteLine($"[{reader}]: disconnected");
        }

        private int HandleFrame(ushort tag, ReadOnlySequence<byte> frame)
        {
            int offset = 0;
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
            }
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
