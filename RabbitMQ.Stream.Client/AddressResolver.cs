using System.Net;

namespace RabbitMQ.Stream.Client
{
    public class AddressResolver
    {
        public AddressResolver(IPEndPoint endPoint)
        {
            EndPoint = endPoint;
            Enabled = true;
        }

        public IPEndPoint EndPoint { get; set; }
        public bool Enabled { get; set; }

    }
}