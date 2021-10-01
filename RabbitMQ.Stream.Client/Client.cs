
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
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
                var supressErrorTask = task.ContinueWith((t, s) =>
                {
                    t.Exception?.Handle(e => true);
                },
                    null,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
                throw new TimeoutException();
            }
        }
    }
    public record ClientParameters
    {
        public IDictionary<string, string> Properties { get; } =
            new Dictionary<string, string>
            {
                {"product", "RabbitMQ Stream"},
                {"version", "0.1.0"},
                {"platform", ".NET"},
                {"copyright", "Copyright (c) 2020-2021 VMware, Inc. or its affiliates."},
                {"information", "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"}
            };
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public EndPoint Endpoint { get; set; } = new IPEndPoint(IPAddress.Loopback, 5552);
        public Action<MetaDataUpdate> MetadataHandler { get; set; } = _ => { };
        public Action<Exception> UnhandledExceptionHandler { get; set; } = _ => { };
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
    
    public class Client
    {
        private uint correlationId = 100; // allow for some pre-amble
        private byte nextPublisherId = 0;
        private readonly ClientParameters parameters;
        private readonly Connection connection;
        //private readonly Channel<ICommand> incoming;
        private readonly Channel<ICommand> outgoing;
        private readonly IDictionary<byte, (Action<ulong[]>, Action<(ulong, ResponseCode)[]>)> publishers =
            new ConcurrentDictionary<byte, (Action<ulong[]>, Action<(ulong, ResponseCode)[]>)>();
        private readonly ConcurrentDictionary<uint, TaskCompletionSource<ICommand>> requests = new();

        private byte nextSubscriptionId;
        private readonly IDictionary<byte, Func<Deliver, Task>> consumers = new ConcurrentDictionary<byte, Func<Deliver, Task>>();

        private object closeResponse;
        private readonly Task outgoingTask;
        private readonly Task incomingTask;
        private int publishCommandsSent;

        public int PublishCommandsSent => publishCommandsSent;

        public int MessagesSent => messagesSent;

        private int messagesSent;
        private int confirmFrames;
        private readonly Channel<ICommand> incoming;

        public int ConfirmFrames => confirmFrames;
        public int IncomingFrames => this.connection.NumFrames;
        //public int IncomingChannelCount => this.incoming.Reader.Count;

        public bool IsClosed => closeResponse != null;

        private Client(ClientParameters parameters, Connection connection, Channel<ICommand> channel)
        {
            this.connection = connection;
            this.incoming = channel;
            this.outgoing = Channel.CreateUnbounded<ICommand>(new UnboundedChannelOptions{SingleReader = false});
            this.parameters = parameters;
            
            // connection.CommandCallback = async (command) =>
            // {
            //     await HandleIncoming(command, parameters.MetadataHandler);
            // };
            //authenticate
            //var ts = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.ExecuteSynchronously);
            incomingTask = Task.Run(() => ProcessIncoming(parameters.MetadataHandler));
            incomingTask.ContinueWith((t, o) => { parameters.UnhandledExceptionHandler(t.Exception); },
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
            outgoingTask = Task.Run(async () =>
            {
                await ProcessOutgoing();
            });
            outgoingTask.ContinueWith((t, o) => { parameters.UnhandledExceptionHandler(t.Exception); },
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        }
        // channels and message publish aggregation
        public static async Task<Client> Create(ClientParameters parameters)
        {
            uint correlationId = 0;
            var channel = Channel.CreateUnbounded<ICommand>(new UnboundedChannelOptions{
                SingleWriter = false, SingleReader = true});
            
            Func<ICommand, Task> callback = (command) =>
            {
                channel.Writer.TryWrite(command);
                return Task.CompletedTask;
            };

            var connection = await Connection.Create(parameters.Endpoint, callback);
            // exchange properties
            var peerProperties = new PeerPropertiesRequest(correlationId, parameters.Properties);
            await connection.Write(peerProperties);
            var peerPropertiesResponse = (PeerPropertiesResponse)await channel.Reader.ReadAsync();
            foreach (var (k, v) in peerPropertiesResponse.Properties)
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

            //channel.Writer.Complete();
            return new Client(parameters, connection, channel);
        }

        public bool Publish(byte publisherId, ulong publishingId, Message message)
        {
            return this.outgoing.Writer.TryWrite(new OutgoingMsg(publisherId, publishingId, message));
        }
        
        public bool Publish(OutgoingMsg msg)
        {
            return this.outgoing.Writer.TryWrite(msg);
        }

        public async Task<(byte, DeclarePublisherResponse)> DeclarePublisher(string publisherRef,
            string stream,
            Action<ulong[]> confirmCallback,
            Action<(ulong, ResponseCode)[]> errorCallback)
        {
            var publisherId = nextPublisherId++;
            publishers.Add(publisherId, (confirmCallback, errorCallback));
            return (publisherId, (DeclarePublisherResponse) await Request(corr =>
                new DeclarePublisherRequest(corr, publisherId, publisherRef, stream)));
        }
        
        public async Task<DeletePublisherResponse> DeletePublisher(byte publisherId)
        {
            var result =  (DeletePublisherResponse)await Request(corr => new DeletePublisherRequest(corr, publisherId));
            publishers.Remove(publisherId);
            return result;
        }


        public async Task<(byte, SubscribeResponse)> Subscribe(string stream, IOffsetType offsetType, ushort initialCredit,
            Dictionary<string, string> properties, Func<Deliver, Task> deliverHandler)
        {
            var subscriptionId = nextSubscriptionId++;
            consumers.Add(subscriptionId, deliverHandler);
            return (subscriptionId,
                (SubscribeResponse)await Request(corr =>
                   new SubscribeRequest(corr, subscriptionId, stream, offsetType, initialCredit, properties)));
        }
        
        public async Task<UnsubscribeResponse> Unsubscribe(byte subscriptionId)
        {
            var result =  (UnsubscribeResponse)await Request(corr => new UnsubscribeRequest(corr, subscriptionId));
            // remove consumer after RPC returns, this should avoid uncorrelated data being sent
            consumers.Remove(subscriptionId);
            return result;
        }
        
        private async Task<ICommand> Request(Func<uint, ICommand> request, int timeout = 10000)
        {
            var corr = NextCorrelationId();
            var tcs = new TaskCompletionSource<ICommand>(TaskCreationOptions.RunContinuationsAsynchronously);
            requests.TryAdd(corr, tcs);
            outgoing.Writer.TryWrite(request(corr));
            await tcs.Task.TimeoutAfter(TimeSpan.FromMilliseconds(timeout));
            return tcs.Task.Result;
        }

        private uint NextCorrelationId()
        {
            return Interlocked.Increment(ref correlationId);
        }

        private async Task ProcessOutgoing()
        {
            const int maxBatchSize = 100; 
            var messages = new List<(ulong, Message)>(maxBatchSize);
            while (true)
            {
                var command = await outgoing.Reader.ReadAsync();
                switch (command)
                {
                    case OutgoingMsg msg:
                        messages.Add((msg.PublishingId, msg.Data));
                        // if the channel is empty or we've reached some num messages limit
                        // send the publish frame
                        // TODO: make limit configurable
                        var readerCount = outgoing.Reader.Count;
                        if (readerCount == 0 || messages.Count >= maxBatchSize)
                        {
                            var publish = new Publish(msg.PublisherId, messages);
                            await connection.Write(publish);
                            this.publishCommandsSent += 1;
                            this.messagesSent += messages.Count;
                            messages.Clear();
                        }
                        
                        break;
                    case { } cmd:
                        await connection.Write(cmd);
                        break;
                }
            }
        }

        private async Task ProcessIncoming(Action<MetaDataUpdate> metadataHandler)
        {
            while (true)
            {
                var cmd = await incoming.Reader.ReadAsync();
                await HandleIncoming(cmd, metadataHandler);
            }
        }

        private async Task HandleIncoming(ICommand command, Action<MetaDataUpdate> metadataHandler)
        {
            switch (command)
            {
                case PublishConfirm confirm:
                    this.confirmFrames += 1;
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
                    await deliverHandler(deliver);
                    break;
                default:
                    if (command.CorrelationId == uint.MaxValue)
                        throw new Exception($"unhandled incoming command {command.GetType()}");
                    else
                        HandleCorrelatedResponse(command);
                    break;
            };
        }

        private void HandleCorrelatedResponse(ICommand command)
        {
            if (requests.TryRemove(command.CorrelationId, out var tsc))
            {
                tsc.SetResult(command);
            }
        }

        public async Task<CloseResponse> Close(string reason)
        {
            if (closeResponse != null)
            {
                return (CloseResponse) closeResponse;
            }

            var result = (CloseResponse) await Request(corr => new CloseRequest(corr, reason));
            closeResponse = result;
            connection.Dispose();

            return result;
        }

        public async Task<QueryPublisherResponse> QueryPublisherSequence(string publisherRef, string stream)
        {
            return (QueryPublisherResponse)await Request(corr => new QueryPublisherRequest(corr, publisherRef, stream));
        }

        public bool StoreOffset(string reference, string stream, ulong offsetValue)
        {
            return outgoing.Writer.TryWrite(new StoreOffsetRequest(stream, reference, offsetValue));
        }

        public async Task<MetaDataResponse> QueryMetadata(string[] streams)
        {
            return (MetaDataResponse)await Request(corr => new MetaDataQuery(corr, streams.ToList()));
        }

        public async Task<QueryOffsetResponse> QueryOffset(string reference, string stream)
        {
            return (QueryOffsetResponse)await Request(corr => new QueryOffsetRequest(stream, corr, reference));
        }

        public async Task<CreateResponse> CreateStream(string stream, IDictionary<string, string> args)
        {
            return (CreateResponse)await Request(corr => new CreateRequest(corr, stream, args));
        }

        public async Task<DeleteResponse> DeleteStream(string stream)
        {
            return (DeleteResponse)await Request(corr => new DeleteRequest(corr, stream));
        }

        public bool Credit(byte subscriptionId, ushort credit)
        {
            return outgoing.Writer.TryWrite(new CreditRequest(subscriptionId, credit));
        }

    }
}
