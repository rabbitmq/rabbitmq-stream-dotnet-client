using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PeerPropertiesRequest : ICommand
    {
        public const ushort Key = 17;
        private readonly uint correlationId;
        private readonly IDictionary<string, string> properties;
        public PeerPropertiesRequest(uint correlationId, IDictionary<string, string> properties)
        {
            this.correlationId = correlationId;
            this.properties = properties;
        }

        public int SizeNeeded
        {
            get
            {
                int size = 12;
                foreach (var (k, v) in properties)
                {
                    // TODO: unnecessary conversion work here to work out the correct size of the frame
                    size += WireFormatting.StringSize(k) + WireFormatting.StringSize(v); //
                }

                return size;
            }
        }

        public void Dispose()
        {
        }

        public int Write(Span<byte> span)
        {
            var command = (ICommand)this;
            int offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span.Slice(offset), command.Version);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), correlationId);
            // map
            offset += WireFormatting.WriteInt32(span.Slice(offset), properties.Count);
            foreach(var (k,v) in properties)
            {
                offset += WireFormatting.WriteString(span.Slice(offset), k);
                offset += WireFormatting.WriteString(span.Slice(offset), v);
            }

            return offset;
        }
    }
    public readonly struct PeerPropertiesResponse : ICommand
    {
        public const ushort Key = 17;
        private readonly uint correlationId;
        private readonly IDictionary<string, string> properties;
        private readonly ushort responseCode;


        public PeerPropertiesResponse(uint correlationId, IDictionary<string, string> properties, ushort responseCode)
        {
            this.responseCode = responseCode;
            this.correlationId = correlationId;
            this.properties = properties;
        }

        public uint CorrelationId => correlationId;
        public IDictionary<string, string> Properties => properties;

        public ushort ResponseCode => responseCode;

        public int SizeNeeded => throw new NotImplementedException();


        internal static int Read(ReadOnlySequence<byte> frame, out ICommand command)
        {
            ushort tag;
            ushort version;
            uint correlation;
            int numProps;
            ushort responseCode;
            var offset = WireFormatting.ReadUInt16(frame, out tag);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out version);
            offset += WireFormatting.ReadUInt32(frame.Slice(offset), out correlation);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out responseCode);
            offset += WireFormatting.ReadInt32(frame.Slice(offset), out numProps);
            var props = new Dictionary<string, string>();
            for (int i = 0; i < numProps; i++)
            {
                string k;
                string v;
                offset += WireFormatting.ReadString(frame.Slice(offset), out k);
                offset += WireFormatting.ReadString(frame.Slice(offset), out v);
                props.Add(k, v);
            }
            command = new PeerPropertiesResponse(correlation, props, responseCode);
            return offset;
        }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }
    }
}
