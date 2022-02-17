namespace RabbitMQ.Stream.Client
{
    public interface INamedEntity
    {
        string ClientProvidedName { get; set; }
    }
}