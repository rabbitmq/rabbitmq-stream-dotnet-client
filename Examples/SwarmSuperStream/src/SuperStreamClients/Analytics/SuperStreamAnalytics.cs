using System.Text;

namespace SuperStreamClients.Analytics;

public partial class SuperStreamAnalytics
{
    private readonly IOptions<RabbitMqStreamOptions> _options;
    public SuperStreamAnalytics(IOptions<RabbitMqStreamOptions> options)
    {
        _options = options;
    }
    private Dictionary<int, AnalyticsInfo> receivedMessages =
     new Dictionary<int, AnalyticsInfo>();

    public IEnumerable<ReceivedCustomerMessage> GetMessagesForCustomer(
        int customerId,
        CancellationToken cancellationToken)
    {
        if (!receivedMessages.ContainsKey(customerId))
            yield break;

        var customerMessages = receivedMessages[customerId];

        foreach (var host in customerMessages.Hosts)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            foreach (var message in host.Value.Messages)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                yield return message;
            }
        }
    }

    public MessageSummary GetMessageSummary(CancellationToken cancellationToken)
    {
        var messageSummary = GetCustomerMessageSummary(cancellationToken);
        var hostSummary = GetHostSummary(cancellationToken);
        return new MessageSummary(
            hostSummary.Sum(x => x.ActiveCustomerCount),
            hostSummary.Where(x => x.Active).Count(),
            hostSummary,
            messageSummary);
    }

    public IEnumerable<HostSummaries> GetHostSummary(
        CancellationToken cancellationToken)
    {
        Dictionary<string, int> hostCount = new Dictionary<string, int>();
        Dictionary<string, int> hostHistoricCount = new Dictionary<string, int>();

        foreach (var customerMessages in receivedMessages)
        {
            if (cancellationToken.IsCancellationRequested)
                yield break;

            var lastHost = customerMessages.Value.Hosts.Last();
            if (!hostCount.ContainsKey(lastHost.Key))
                hostCount.Add(lastHost.Key, 0);

            hostCount[lastHost.Key]++;

            foreach (var host in customerMessages.Value.Hosts)
            {
                if (!hostHistoricCount.ContainsKey(host.Key))
                    hostHistoricCount.Add(host.Key, 0);

                hostHistoricCount[host.Key]++;
            }
        }

        var hostSummaries = new List<HostSummaries>();

        foreach (var hostCountSummary in hostHistoricCount)
        {
            var activeCount = hostCount.ContainsKey(hostCountSummary.Key) ? hostCount[hostCountSummary.Key] : 0;

            hostSummaries.Add(new HostSummaries(
                activeCount > 0,
                hostCountSummary.Key,
                activeCount,
                hostCountSummary.Value));
        }

        foreach (var ordered in hostSummaries
            .OrderByDescending(x => x.ActiveCustomerCount)
            .ThenBy(x => x.HostName))
        {
            yield return ordered;
        }
    }

    public IEnumerable<CustomerSummary> GetCustomerMessageSummary(
        CancellationToken cancellationToken)
    {

        foreach (var customerMessages in receivedMessages.OrderBy(x => x.Key))
        {
            var customerHostSummary = new List<CustomerHostSummary>();

            if (cancellationToken.IsCancellationRequested)
                yield break;

            foreach (var host in customerMessages.Value.Hosts)
            {
                if (cancellationToken.IsCancellationRequested)
                    yield break;

                var summary = new CustomerHostSummary(
                    customerMessages.Key,
                    host.Key,
                    host.Value.Messages.Count,
                    host.Value.Messages.Last().CustomerMessage.MessageNumber,
                    host.Value.Messages.Last().TimeReceived);

                customerHostSummary.Add(summary);
            }

            var customerSummary = new CustomerSummary(
                customerMessages.Key,
                customerHostSummary.Count,
                customerHostSummary.Sum(x => x.NumberOfMessages),
                customerHostSummary.Last().LastMessageNumber,
                customerHostSummary.Last().LastMessage,
                customerHostSummary
                );

            yield return customerSummary;
        }
    }

    public void NewMessage(ReceivedCustomerMessage message)
    {
        if (!receivedMessages.ContainsKey(message.CustomerMessage.CustomerId))
        {
            receivedMessages.Add(message.CustomerMessage.CustomerId, new AnalyticsInfo());
        }
        var customerAnalytics = receivedMessages[message.CustomerMessage.CustomerId];

        if (!customerAnalytics.Hosts.ContainsKey(message.ReceivedOnHost))
            customerAnalytics.Hosts.Add(message.ReceivedOnHost, new HostMessages()
            {
                Host = message.ReceivedOnHost,
                TimeSeen = TimeOnly.FromDateTime(DateTime.Now)
            });

        var host = customerAnalytics.Hosts[message.ReceivedOnHost];

        host.Messages.Add(message);

        if (!customerAnalytics.Sources.Contains(message.SourceStream))
            customerAnalytics.Sources.Add(message.SourceStream);
    }

    public string GenerateSummaryInfo(CancellationToken cancellationToken)
    {
        var options = _options.Value;
        var maxMessagesToShow = options.MaxMessagesToShowInAnalytics;
        var sb = new StringBuilder();
        var orderedCustomers = receivedMessages.OrderBy(x => x.Key);
        var infoFound = false;

        foreach (var customer in orderedCustomers)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            sb.Append("Customer: ");
            sb.AppendLine(customer.Key.ToString());

            sb.Append("Sources: ");
            foreach (var source in customer.Value.Sources)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                sb.Append(source);
                sb.Append(", ");
            }
            sb.AppendLine();

            foreach (var host in customer.Value.Hosts)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                var hostMesages = host.Value.Messages;
                sb.Append("Host (First message received at ");
                sb.Append(host.Value.TimeSeen.ToLongTimeString());
                sb.Append("): ");
                sb.AppendLine(host.Key.ToString());

                sb.Append("Messages (");
                sb.Append(hostMesages.Count.ToString());
                sb.Append("): ");
                if (hostMesages.Count > maxMessagesToShow)
                    sb.Append("...");

                foreach (var message in hostMesages.TakeLast(maxMessagesToShow))
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    infoFound = true;
                    sb.Append(message.CustomerMessage.MessageNumber);
                    sb.Append("(");
                    sb.Append(message.TimeReceived.ToLongTimeString());
                    sb.Append("), ");
                }

                sb.AppendLine();
            }

            sb.AppendLine();
        }
        if (!infoFound)
            sb.AppendLine("Waiting on analytic data to arrive.");

        return sb.ToString();
    }
}