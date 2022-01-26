using System;
using System.Buffers;
using System.Collections.Generic;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Message
    {
        private readonly AmqpFrameHeader amqpFrameHeader;

        public Message(byte[] data) : this(new Data(new ReadOnlySequence<byte>(data)))
        {
        }
        
        public Message(Data data, Annotations? annotations = null, Properties? properties =null)
        {
            this.Data = data;
            this.Annotations = annotations;
            this.Properties = properties;
            amqpFrameHeader = new AmqpFrameHeader(0,
                AmqpType.TypeCodeSmallUlong,
                FrameType.TypeCodeApplicationData);
        }


        public Annotations? Annotations { get; }
        public Properties? Properties { get; }

        public Data Data { get; }
        public int Size => Data.Size + amqpFrameHeader.Size;

        public int Write(Span<byte> span)
        {
            var offset = amqpFrameHeader.Write(span);
            offset += Data.Write(span.Slice(offset));
            return offset;
        }

        public ReadOnlySequence<byte> Serialize()
        {
            //what a massive cludge
            var data = new byte[Data.Size];
            Data.Write(data);
            return new ReadOnlySequence<byte>(data);
        }

        public static Message From(ReadOnlySequence<byte> amqpData)
        {
            //                                                         Bare Message
            //                                                             |
            //                                       .---------------------+--------------------.
            //                                      |                                           |
            // +--------+-------------+-------------+------------+--------------+--------------+--------
            // | header | delivery-   | message-    | properties | application- | application- | footer |
            // |        | annotations | annotations |             | properties  | data         |        |
            // +--------+-------------+-------------+------------+--------------+--------------+--------+            

            //parse AMQP encoded data
            var offset = 0;
            Annotations? annotations = null;
            AMQP.Data? data = null;
            while (amqpData.Slice(offset).Length != 0)
            {
                var messageType = AMQP.AmqpFrameHeader.Parse(amqpData.Slice(offset));
                offset += messageType.Size;
                switch (messageType.DataCode)
                {
                    case FrameType.TypeCodeApplicationData:
                        data = AMQP.Data.Parse(amqpData.Slice(offset));
                        offset += data.Value.Size;
                        break;
                    case FrameType.TypeCodeMessageAnnotations:
                        annotations = AMQP.Annotations.Parse(amqpData.Slice(offset));
                        offset += annotations.Value.Size;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }


            if (data != null)
                return new Message(data.Value, annotations);

            throw new AMQP.AmqpParseException($"Can't parse data is null");
        }
    }
}