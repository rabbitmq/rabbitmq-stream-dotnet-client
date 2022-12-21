namespace SuperStreamClients.Analytics;

public record CustomerSummary(int CustomerId, int DifferentHostCount, int NumberOfMessages, int LastMessageNumber, TimeOnly LastMessage, IEnumerable<CustomerHostSummary> CustomerHostSummaries);

