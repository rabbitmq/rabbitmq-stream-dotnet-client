using System;
using System.Buffers;
using System.Collections.Generic;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public struct Hello
    {
    }

    public class Message
    {
        public Message(byte[] data) : this(new Data(new ReadOnlySequence<byte>(data)))
        {
        }


        public Message(Data data)
        {
            this.Data = data;
        }

        public Annotations Annotations { get; set; }

        //
        public Properties Properties { get; set; }
        public Data Data { get; }
        public int Size => Data.Size + (Properties?.Size ?? 0);

        public int Write(Span<byte> span)
        {
            var offset = 0;
            if (Properties != null)
            {
                offset += Properties.Write(span);
            }

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
            Annotations annotations = null;
            AMQP.Data data = default;
            Properties properties = default;
            while (offset != amqpData.Length)
            {
                var dataCode = Described.ExtractCode(amqpData.Slice(offset));
                switch (dataCode)
                {
                    case Codec.ApplicationData:
                        offset += Described.DecoderSize;
                        data = Data.Parse(amqpData.Slice(offset), out var readD);
                        offset += readD;
                        break;
                    case Codec.MessageAnnotations:
                        offset += Described.DecoderSize;
                        annotations = Annotations.Parse(amqpData.Slice(offset), out var readA);
                        offset += readA;
                        break;
                    case Codec.MessageProperties:
                        properties = Properties.Parse(amqpData.Slice(offset), out var readP);
                        offset += readP;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            var msg = new Message(data)
            {
                Annotations = annotations,
                Properties = properties
            };
            return msg;
        }
    }
}