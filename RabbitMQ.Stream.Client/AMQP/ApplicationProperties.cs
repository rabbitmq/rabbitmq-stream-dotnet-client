namespace RabbitMQ.Stream.Client.AMQP
{
    public class ApplicationProperties : Map<string>
    {
        public ApplicationProperties() : base()
        {
            MapDataCode = AMQP.DescribedFormatCode.ApplicationProperties;
        }
        
    }
}