using System;
using System.Buffers;
using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public class Message
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
            //parse AMQP encoded data
            var data = AMQP.Data.Parse(amqpData);
            return new Message(data);
        }
    }
}