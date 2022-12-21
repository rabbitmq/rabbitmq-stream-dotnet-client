namespace SuperStreamClients;

public record CustomerMessage(string Host, int CustomerId, Guid MessageId, int MessageNumber, string Data);
