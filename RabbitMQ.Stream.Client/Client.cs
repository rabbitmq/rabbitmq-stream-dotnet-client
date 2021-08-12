
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    internal static class TaskExtensions
    {
        public static async Task TimeoutAfter(this Task task, TimeSpan timeout)
        {
            if (task == await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false))
            {
                await task.ConfigureAwait(false);
            }
            else
            {
                var supressErrorTask = task.ContinueWith((t, s) => t.Exception.Handle(e => true), null, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                throw new TimeoutException();
            }
        }
    }
    public class ClientParameters
    {
        public IDictionary<string, string> Properties { get; set; } = new Dictionary<string, string>{{"key", "value"}};
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };
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
        private uint correlationId = 100; // allow for some pre-amble
        private byte nextPublisherId = 0;
        private readonly ClientParameters parameters;
        private readonly Connection connection;
        private readonly Channel<ICommand> incoming;
        private readonly Channel<Object> outgoing;
        private readonly IDictionary<byte, (Action<ulong[]>, Action<(ulong, ushort)[]>)> publishers =
            new ConcurrentDictionary<byte, (Action<ulong[]>, Action<(ulong, ushort)[]>)>();
        private readonly IDictionary<uint, TaskCompletionSource<ICommand>> requests =
            new ConcurrentDictionary<uint, TaskCompletionSource<ICommand>>();

        private byte nextSubscriptionId;
        private readonly IDictionary<byte, Action<Deliver>> consumers = new ConcurrentDictionary<byte, Action<Deliver>>();

        public bool IsClosed { get; private set; } = false;

        private Client(ClientParameters parameters, Connection connection, Channel<ICommand> channel)
        {
            this.connection = connection;
            this.incoming = channel;
            this.outgoing = Channel.CreateUnbounded<object>();
            this.parameters = parameters;
            //authenticate
            var ts = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.AttachedToParent);
            _ = ts.StartNew(ProcessOutgoing);
            _ = ts.StartNew(() => ProcessIncoming(parameters.MetadataHandler));
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

        public async Task<(byte, DeclarePublisherResponse)> DeclarePublisher(string publisherRef,
        string stream, Action<ulong[]> confirmCallback, Action<(ulong, ushort)[]> errorCallback)
        {
            var publisherId = nextPublisherId++;
            publishers.Add(publisherId, (confirmCallback, errorCallback));
            return (publisherId, (DeclarePublisherResponse)await Request(corr =>
            new DeclarePublisherRequest(corr, publisherId, publisherRef, stream)));
        }

        public async Task<(byte, SubscribeResponse)> Subscribe(string stream, OffsetTypeFirst offsetType, ushort initialCredit,
            Dictionary<string, string> properties, Action<Deliver> deliverHandler)
        {
            var subscriptionId = nextSubscriptionId++;
            consumers.Add(subscriptionId, deliverHandler);
            return (subscriptionId,
                (SubscribeResponse) await Request(corr =>
                    new SubscribeRequest(corr, subscriptionId, stream, offsetType, initialCredit, properties)));
        }
        private async Task<ICommand> Request(Func<uint, ICommand> request)
        {
            var corr = NextCorrelationId();
            var tcs = new TaskCompletionSource<ICommand>();
            requests.Add(corr, tcs);
            outgoing.Writer.TryWrite(request(corr));
            await tcs.Task.TimeoutAfter(TimeSpan.FromMilliseconds(10000));
            return tcs.Task.Result;
        }

        private uint NextCorrelationId()
        {
            return Interlocked.Increment(ref correlationId);
        }

        private async Task ProcessOutgoing()
        {
            var msgs = new List<(ulong, ReadOnlySequence<byte>)>();
            while (true)
            {
                var command = await outgoing.Reader.ReadAsync();
                var readerCount = outgoing.Reader.Count;
                switch(command)
                {
                    case ICommand cmd:
                        await this.connection.Write((ICommand) cmd);
                        break;
                    case OutgoingMsg msg:
                        msgs.Add((msg.PublishingId, msg.Data));
                        // if the channel is empty or we've reached some num msgs limit
                        // send the publish frame
                        if(readerCount == 0 || msgs.Count >= 1000)
                        {
                            var publish = new Publish(msg.PublisherId, msgs);
                            // Console.WriteLine($"publishing {msgs.Count} message batch {readerCount}");
                            await this.connection.Write(publish);
                            msgs.Clear();
                        }
                        break;
                }
            }
        }

        private async Task ProcessIncoming(Action<MetaDataUpdate> metadataHandler)
        {
            while (true)
            {
                var cmd = await incoming.Reader.ReadAsync();
                // Console.WriteLine($"incoming command {cmd}");
                HandleIncoming(cmd, metadataHandler);
            }
        }

        private bool HandleIncoming(ICommand command, Action<MetaDataUpdate> metadataHandler)
        {
            switch (command)
            {
                case PublishConfirm confirm:
                    var (confirmCallback, _) = publishers[confirm.PublisherId];
                    confirmCallback(confirm.PublishingIds);
                    break;
                case PublishError error:
                    var (_, errorCallback) = publishers[error.PublisherId];
                    errorCallback(error.PublishingErrors);
                    break;
                case MetaDataUpdate metaDataUpdate:
                    metadataHandler(metaDataUpdate);
                    break;
                case Deliver deliver:
                    var deliverHandler = consumers[deliver.SubscriptionId];
                    deliverHandler(deliver);
                    break;
                default:
                    if(command.CorrelationId == uint.MaxValue)
                        throw new Exception($"unhandled incoming command {command.GetType()}");
                    else
                        HandleCorrelatedResponse(command);
                    break;
                
            };

            return true;
        }

        private void HandleCorrelatedResponse(ICommand command)
        {
            if (requests.ContainsKey(command.CorrelationId))
            {
                var tsc = requests[correlationId];
                requests.Remove(correlationId);
                tsc.SetResult(command);
            }
        }

        public async Task<CloseResponse> Close(string reason)
        {
            if(IsClosed)
            {
                throw new Exception("already closed");
            }
            var result = (CloseResponse)await Request(corr => new CloseRequest(corr, reason)); 
            connection.Dispose();
            this.IsClosed = true;
            return result;
        }

        public async Task<QueryPublisherResponse> QueryPublisherSequence(string publisherRef, string stream)
        {
            return (QueryPublisherResponse) await Request(corr => new QueryPublisherRequest(corr, publisherRef, stream)) ;
        }

        public async Task<DeletePublisherResponse> DeletePublisher(byte publisherId)
        {
            return (DeletePublisherResponse) await Request(corr => new DeletePublisherRequest(corr, publisherId)) ;
        }

        public async Task<MetaDataResponse> QueryMetadata(string[] streams)
        {
            return (MetaDataResponse) await Request(corr => new MetaDataQuery(corr, streams.ToList())) ;
        }

        public async Task<CreateResponse> CreateStream(string stream, IDictionary<string, string> args)
        {
            return (CreateResponse) await Request(corr => new CreateRequest(corr, stream, args)) ;
        }

        public async Task<DeleteResponse> DeleteStream(string stream)
        {
            return (DeleteResponse) await Request(corr => new DeleteRequest(corr, stream)) ;
        }

        public bool Credit(byte subscriptionId, ushort credit)
        {
            return outgoing.Writer.TryWrite(new CreditRequest(subscriptionId, credit));
        }
    }
}
