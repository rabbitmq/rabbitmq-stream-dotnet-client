
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public class ClientParameters
    {
        public IDictionary<string, string> Properties { get; set; } = new Dictionary<string, string>{{"key", "value"}};
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
    }

    public readonly struct OutgoingMsg
    {
        private readonly byte publisherId;
        private readonly ulong publishingId;
        private readonly ReadOnlySequence<byte> data;

        public OutgoingMsg(byte publisherId, ulong publishingId, ReadOnlySequence<byte> data)
        {
            this.publisherId = publisherId;
            this.publishingId = publishingId;
            this.data = data;
        }

        public byte PublisherId => publisherId;

        public ulong PublishingId => publishingId;

        public ReadOnlySequence<byte> Data => data;
    }
    public class Client
    {
        private uint correlationId = 0;
        private byte nextPublisherId = 0;
        private readonly ClientParameters parameters;
        private Connection connection;
        private Channel<ICommand> incoming;
        private Channel<Object> outgoing;
        private IDictionary<byte, Action<ulong[]>> publishers = new ConcurrentDictionary<byte, Action<ulong[]>>();
        private IDictionary<uint, TaskCompletionSource<ICommand>> requests = new ConcurrentDictionary<uint, TaskCompletionSource<ICommand>>();

        private Client(ClientParameters parameters, Connection connection, Channel<ICommand> channel)
        {
            this.connection = connection;
            this.incoming = channel;
            this.outgoing = Channel.CreateUnbounded<Object>();
            this.parameters = parameters;
            //authenticate
            var ts = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.AttachedToParent);
            _ = ts.StartNew(ProcessOutgoing);
            _ = ts.StartNew(ProcessIncoming);
        }
        // channels and message publish aggregation
        public static async Task<Client> Create(ClientParameters parameters)
        {
           uint correlationId = 0;
            var channel = Channel.CreateUnbounded<ICommand>();
            Action<ICommand> callback = (command) =>
            {
                channel.Writer.TryWrite(command);
            };
            var connection = new Connection(callback);
            // exchange properties
            var peerProperties = new PeerPropertiesRequest(correlationId, parameters.Properties);
            await connection.Write(peerProperties);
            var peerPropertiesResponse = (PeerPropertiesResponse)await channel.Reader.ReadAsync();
            foreach(var (k, v) in peerPropertiesResponse.Properties)
                Console.WriteLine($"server Props {k} {v}");
            //auth
            await connection.Write(new SaslHandshakeRequest(++correlationId));
            var saslHandshakeResponse = (SaslHandshakeResponse)await channel.Reader.ReadAsync();
            foreach (var m in saslHandshakeResponse.Mechanisms)
                Console.WriteLine($"sasl mechanism: {m}");

            var saslData = Encoding.UTF8.GetBytes($"\0{parameters.UserName}\0{parameters.Password}");
            await connection.Write(new SaslAuthenticateRequest(++correlationId, "PLAIN", saslData));
            var authResponse = (SaslAuthenticateResponse)await channel.Reader.ReadAsync();
            Console.WriteLine($"auth: {authResponse.ResponseCode} {authResponse.Data}");

            //tune
            await connection.Write(new TuneRequest(0, 0));
            var tune = (TuneResponse)await channel.Reader.ReadAsync();
            // open 
            await connection.Write(new OpenRequest(++correlationId, "/"));
            var open = (OpenResponse)await channel.Reader.ReadAsync();
            Console.WriteLine($"open: {open.ResponseCode} {open.ConnectionProperties.Count}");
            foreach (var (k, v) in open.ConnectionProperties)
                Console.WriteLine($"open prop: {k} {v}");

            return new Client(parameters, connection, channel);
        }

        public bool Publish(OutgoingMsg msg)
        {
            return this.outgoing.Writer.TryWrite(msg);
        }

        public async Task<DeclarePublisherResponse> DeclarePublisher(string publisherRef, string stream, Action<ulong[]> confirmCallback)
        {
            var corr = ++correlationId; //TODO: use interlocked here
            var tcs = new TaskCompletionSource<ICommand>();
            requests.Add(corr, tcs);
            var publisherId = nextPublisherId++;
            publishers.Add(publisherId, confirmCallback);
            outgoing.Writer.TryWrite(new DeclarePublisherRequest(corr, publisherId, publisherRef, stream));
            var res = (DeclarePublisherResponse)await tcs.Task;
            return res;
        }

        private async Task ProcessOutgoing()
        {
            var msgs = new List<OutgoingMsg>();
            while (true)
            {
                var cmd = await outgoing.Reader.ReadAsync();
                var readerCount = outgoing.Reader.Count;
                if(cmd is ICommand) 
                    await this.connection.Write((ICommand) cmd);
                else if(cmd is OutgoingMsg)
                {
                    var msg = (OutgoingMsg) cmd;
                    msgs.Add(msg);
                    // if the channel is empty or we've reached some num msgs limit
                    // send the publish frame
                    if(readerCount == 0 || msgs.Count >= 1000)
                    {
                        var outMsgs = msgs.Select(o => (o.PublishingId, o.Data)).ToList();
                        msgs.Clear();
                        var p = new Publish(msg.PublisherId, outMsgs);
                        Console.WriteLine($"publishing {outMsgs.Count} message batch {readerCount}");
                        await this.connection.Write(p);
                    }
                }
            }
        }

        private async Task ProcessIncoming()
        {
            while (true)
            {
                var cmd = await incoming.Reader.ReadAsync();
                // Console.WriteLine($"incoming command {cmd}");
                HandleIncoming(cmd);
            }
        }

        private bool HandleIncoming(ICommand command)
        {
            switch (command)
            {
                case DeclarePublisherResponse response:
                    HandleCorrelatedResponse(command, response.CorrelationId);
                    break;
                case PublishConfirm confirm:
                    var confirmCallback = publishers[confirm.PublisherId];
                    confirmCallback(confirm.PublishingIds);
                    break;
            };
            return true;
        }

        private void HandleCorrelatedResponse(ICommand command, uint correlationId)
        {
            if (requests.ContainsKey(correlationId))
            {
                var tsc = requests[correlationId];
                requests.Remove(correlationId);
                tsc.SetResult(command);
            }
        }
    }
}
