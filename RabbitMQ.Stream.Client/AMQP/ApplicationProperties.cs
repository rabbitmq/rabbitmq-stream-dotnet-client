namespace RabbitMQ.Stream.Client.AMQP
{
    public class ApplicationProperties : Map<string>
    {
        public ApplicationProperties() : base()
        {
            DataCode = Codec.ApplicationProperties;
        }
        
    }
}