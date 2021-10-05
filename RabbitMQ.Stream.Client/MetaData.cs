using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    public readonly struct MetaDataQuery : ICommand
    {
        public const ushort Key = 15;
        private readonly uint correlationId;
        private readonly IEnumerable<string> streams;
        public MetaDataQuery(uint correlationId, IList<string> streams)
        {
            this.correlationId = correlationId;
            this.streams = streams.ToList();
        }

        public int SizeNeeded
        {
            get
            {
                int size = 12;
                foreach (var s in streams)
                {
                    // TODO: unnecessary conversion work here to work out the correct size of the frame
                    size += WireFormatting.StringSize(s); //
                }

                return size;
            }
        }
        
        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            // map
            offset += WireFormatting.WriteInt32(span.Slice(offset), streams.Count());
            foreach(var s in streams)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), s);
            }

            return offset;
        }
    }

    public readonly struct Broker
    {
        private readonly string host;
        private readonly uint port;

        public string Host => host;

        public uint Port => port;

        public Broker(string host, uint port)
        {
            this.host = host;
            this.port = port;
        }
    }
    
    public readonly struct StreamInfo
    {
        public string Stream { get; }
        public ushort Code { get; }
        public Broker Leader { get; }
        public IList<Broker> Replicas { get; }

        public StreamInfo(string stream, ushort code, Broker leader, IList<Broker> replicas)
        {
            Stream = stream;
            Code = code;
            Leader = leader;
            Replicas = replicas;
        }
    }
    
    public readonly struct MetaDataResponse : ICommand
    {
        public const ushort Key = 15;
        private readonly uint correlationId;
        private readonly IDictionary<string, StreamInfo> streamInfos;


        public MetaDataResponse(uint correlationId, IDictionary<string, StreamInfo> streamInfos)
        {
            this.streamInfos = streamInfos;
            this.correlationId = correlationId;
        }

        public uint CorrelationId => correlationId;

        public int SizeNeeded => throw new NotImplementedException();

        public IDictionary<string, StreamInfo> StreamInfos => streamInfos;

        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numBrokers);
            var brokers = new Dictionary<ushort, Broker>();
            for (int i = 0; i < numBrokers; i++)
            {
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var brokerRef);
                offset += WireFormatting.ReadString(frame.Slice(offset), out var host);
                offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var port);
                brokers.Add(brokerRef, new Broker(host, port));
            }
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numStreams);
            var streamInfos = new Dictionary<string, StreamInfo>();
            for (int i = 0; i < numStreams; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out var stream);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var code);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var leaderRef);
                offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numReplicas);
                ushort[] replicaRefs = new ushort[numReplicas];
                for (var j = 0; j < numReplicas; j++)
                {
                    offset += WireFormatting.ReadUInt16(frame.Slice(offset), out replicaRefs[j]);
                }

                var replicas = replicaRefs.Select(r => brokers[r]).ToList();
                var leader = brokers[leaderRef];
                streamInfos.Add(stream, new StreamInfo(stream, code, leader, replicas));
            }
            command = new MetaDataResponse(correlation, streamInfos);
            
            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
    
    public readonly struct MetaDataUpdate : ICommand
    {
        public const ushort Key = 16;
        private readonly ResponseCode code;
        private readonly string stream;

        public MetaDataUpdate(string stream, ResponseCode code)
        {
            this.stream = stream;
            this.code = code;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public ResponseCode Code => code;

        public string Stream => stream;

        internal static int Read(ReadOnlySequence<byte> frame, out MetaDataUpdate command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var code);
            offset += WireFormatting.ReadString(frame.Slice(offset), out var stream);
            command = new MetaDataUpdate(stream, (ResponseCode)code);
            
            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
