using System;
using System.Buffers;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client.AMQP
{
    public class Annotations : Map<object>
    {
        public Annotations() : base()
        {
            MapDataCode = AMQP.DescribedFormatCode.MessageAnnotations;
        }
    }
}