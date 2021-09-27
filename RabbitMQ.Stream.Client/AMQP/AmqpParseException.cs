using System;

namespace RabbitMQ.Stream.Client.AMQP
{
    public class AmqpParseException : Exception
    {
        public AmqpParseException(string s) : base(s)
        {
        }
    }
}