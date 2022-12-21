namespace SuperStreamClients;

public record CustomerMessage(string Host, int CustomerId, Guid MessageId, int MessageNumber, string Data);

public record ReceivedCustomerMessage(string ReceivedOnHost, string SourceStream, CustomerMessage CustomerMessage);