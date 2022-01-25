using System;
using System.Buffers;
using System.Collections.Generic;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public readonly struct Message
    {
        private readonly Properties properties;
        public Message(byte[] data) : this(new Data(new ReadOnlySequence<byte>(data)))
        {
        }

        public Message(Data data, Properties properties = new Properties())
        {
            this.Data = data;
            this.properties = properties;
        }

        public Data Data { get; }
        public int Size => Data.Size;

        public int Write(Span<byte> span)
        {
            return Data.Write(span);
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
            var messageType = AMQP.MessageType.Parse(amqpData);
            var offest = messageType.Size;
            switch (messageType.DataCode)
            {
                case FrameType.TypeCodeApplicationData:
                    
                    break;
                case FrameType.TypeCodeMessageAnnotations:
                    var annotations = AMQP.Annotations.Parse(amqpData.Slice(offest));

                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            var data = AMQP.Data.Parse(amqpData.Slice(offest), messageType);
            return new Message(data);
        }
    }
}