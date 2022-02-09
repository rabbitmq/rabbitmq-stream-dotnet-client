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

        public Annotations Annotations { get; internal set; }

        public ApplicationProperties ApplicationProperties { get; set; }

        public Properties Properties { get; set; }
        public Data Data { get; }

        public int Size => Data.Size +
                           (Properties?.Size ?? 0) +
                           (Annotations?.Size ?? 0) +
                           (ApplicationProperties?.Size ?? 0);

        public int Write(Span<byte> span)
        {
            var offset = 0;
            if (Properties != null)
            {
                offset += Properties.Write(span.Slice(offset));
            }

            if (ApplicationProperties != null)
            {
                offset += ApplicationProperties.Write(span.Slice(offset));
            }

            if (Annotations != null)
            {
                offset += Annotations.Write(span.Slice(offset));
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
            Data data = default;
            Properties properties = null;
            ApplicationProperties applicationProperties = null;
            while (offset != amqpData.Length)
            {
                var dataCode = Described.ReadDataCode(amqpData.Slice(offset));
                switch (dataCode)
                {
                    case Codec.ApplicationData:
                        offset += Described.Size;
                        data = Data.Parse(amqpData.Slice(offset), ref offset);
                        break;
                    case Codec.MessageAnnotations:
                        offset += Described.Size;
                        annotations = Annotations.Parse<Annotations>(amqpData.Slice(offset), ref offset);
                        break;
                    case Codec.MessageProperties:
                        properties = Properties.Parse(amqpData.Slice(offset), ref offset);
                        break;
                    case Codec.ApplicationProperties:
                        offset += Described.Size;
                        applicationProperties =
                            ApplicationProperties.Parse<ApplicationProperties>(amqpData.Slice(offset), ref offset);
                        break;
                    default:
                        Console.WriteLine($"dataCode: {dataCode} not handled. Please open an issue.");
                        throw new ArgumentOutOfRangeException($"dataCode: {dataCode} not handled");
                }
            }

            var msg = new Message(data)
            {
                Annotations = annotations,
                Properties = properties,
                ApplicationProperties = applicationProperties
            };
            return msg;
        }
    }
}