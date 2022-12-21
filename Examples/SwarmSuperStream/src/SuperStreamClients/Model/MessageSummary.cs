namespace SuperStreamClients.Analytics;

public record MessageSummary(int ActiveCustomerCount, int ActiveConsumerCount, IEnumerable<HostSummaries> HostSummaries, IEnumerable<CustomerSummary> CustomerSummaries);

