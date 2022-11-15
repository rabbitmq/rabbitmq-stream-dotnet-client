﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

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
                var size = 12;
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
            offset += WireFormatting.WriteUInt16(span[offset..], command.Version);
            offset += WireFormatting.WriteUInt32(span[offset..], correlationId);
            // map
            offset += WireFormatting.WriteInt32(span[offset..], streams.Count());
            foreach (var s in streams)
            {
                offset += WireFormatting.WriteString(span[offset..], s);
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
        public ResponseCode ResponseCode { get; }
        public Broker Leader { get; }
        public IList<Broker> Replicas { get; }

        public StreamInfo(string stream, ResponseCode responseCode, Broker leader, IList<Broker> replicas)
        {
            Stream = stream;
            ResponseCode = responseCode;
            Leader = leader;
            Replicas = replicas;
        }

        public StreamInfo(string stream, ResponseCode responseCode)
        {
            Stream = stream;
            ResponseCode = responseCode;
            Leader = default;
            Replicas = null;
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

        internal static int Read(ReadOnlySequence<byte> frame, out MetaDataResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out var tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var correlation);
            //offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numBrokers);
            var brokers = new Dictionary<short, Broker>();
            for (var i = 0; i < numBrokers; i++)
            {
                offset += WireFormatting.ReadInt16(frame.Slice(offset), out var brokerRef);
                offset += WireFormatting.ReadString(frame.Slice(offset), out var host);
                offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var port);
                brokers.Add(brokerRef, new Broker(host, port));
            }

            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numStreams);
            var streamInfos = new Dictionary<string, StreamInfo>();
            for (var i = 0; i < numStreams; i++)
            {
                offset += WireFormatting.ReadString(frame.Slice(offset), out var stream);
                offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var code);

                offset += WireFormatting.ReadInt16(frame.Slice(offset), out var leaderRef);
                offset += WireFormatting.ReadUInt32(frame.Slice(offset), out var numReplicas);
                var replicaRefs = new short[numReplicas];
                for (var j = 0; j < numReplicas; j++)
                {
                    offset += WireFormatting.ReadInt16(frame.Slice(offset), out replicaRefs[j]);
                }

                if (brokers.Count > 0)
                {
                    var replicas = replicaRefs.Select(r => brokers[r]).ToList();
                    var leader = brokers.ContainsKey(leaderRef) ? brokers[leaderRef] : default;
                    streamInfos.Add(stream, new StreamInfo(stream, (ResponseCode)code, leader, replicas));
                }
                else
                {
                    streamInfos.Add(stream, new StreamInfo(stream, (ResponseCode)code));
                }
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
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
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
