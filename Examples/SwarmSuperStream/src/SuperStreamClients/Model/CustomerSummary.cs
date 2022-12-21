namespace SuperStreamClients.Analytics;

    public record MessageSummary(IEnumerable<HostSummaries> HostSummaries, IEnumerable<CustomerSummary> CustomerSummaries);

    public record HostSummaries(bool Active, string HostName, int ActiveCustomerCount, int AllTimeCustomerCount);

    

    public record CustomerSummary(int CustomerId, int DifferentHostCount, int NumberOfMessages, int LastMessageNumber, TimeOnly LastMessage, IEnumerable<CustomerHostSummary> CustomerHostSummaries);

    public record CustomerHostSummary(int CustomerId, string HostId, int NumberOfMessages, int LastMessageNumber, TimeOnly LastMessage);

