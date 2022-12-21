namespace SuperStreamClients;

public record ReceivedCustomerMessage(TimeOnly TimeReceived, string ReceivedOnHost, string SourceStream, CustomerMessage CustomerMessage);
