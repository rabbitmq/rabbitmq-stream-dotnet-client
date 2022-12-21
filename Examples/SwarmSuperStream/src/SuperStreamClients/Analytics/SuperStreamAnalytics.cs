using System.Text;

namespace SuperStreamClients.Analytics;

public class SuperStreamAnalytics
{
    private readonly IOptions<RabbitMqStreamOptions> _options;
    public SuperStreamAnalytics(IOptions<RabbitMqStreamOptions> options)
    {
        _options = options;
    }
    private Dictionary<int, AnalyticsInfo> receivedMessages =
     new Dictionary<int, AnalyticsInfo>();

    public void NewMessage(ReceivedCustomerMessage message)
    {
        if (!receivedMessages.ContainsKey(message.CustomerMessage.CustomerId))
        {
            receivedMessages.Add(message.CustomerMessage.CustomerId, new AnalyticsInfo());
        }
        var customerAnalytics = receivedMessages[message.CustomerMessage.CustomerId];

        if(!customerAnalytics.Hosts.ContainsKey(message.ReceivedOnHost))
            customerAnalytics.Hosts.Add(message.ReceivedOnHost, new HostMessages(){
                Host = message.ReceivedOnHost,
                TimeSeen = TimeOnly.FromDateTime(DateTime.Now)});

        var host = customerAnalytics.Hosts[message.ReceivedOnHost];

        host.Messages.Add(message);

        if (!customerAnalytics.Sources.Contains(message.SourceStream))
            customerAnalytics.Sources.Add(message.SourceStream);
    }

    public override string ToString()
    {
        var options = _options.Value;
        var maxMessagesToShow = options.MaxMessagesToShowInAnalytics;
        var sb = new StringBuilder();
        var orderedCustomers = receivedMessages.OrderBy(x => x.Key);
        var infoFound = false;

        foreach (var customer in orderedCustomers)
        {
            sb.Append("Customer: ");
            sb.AppendLine(customer.Key.ToString());

            sb.Append("Sources: ");
            foreach (var source in customer.Value.Sources)
            {
                sb.Append(source);
                sb.Append(", ");
            }
            sb.AppendLine();

            foreach (var host in customer.Value.Hosts)
            {
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
        if(!infoFound)
            sb.AppendLine("Waiting on analytic data to arrive.");

        return sb.ToString();
    }
}