namespace SuperStreamClients.Analytics;

public record CustomerHostSummary(int CustomerId, string HostId, int NumberOfMessages, int LastMessageNumber, TimeOnly LastMessage);

