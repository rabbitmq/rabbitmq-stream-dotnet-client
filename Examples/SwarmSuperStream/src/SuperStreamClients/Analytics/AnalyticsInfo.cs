namespace SuperStreamClients.Analytics;

public class AnalyticsInfo
{
    public List<string> Sources = new List<string>();
    public Dictionary<string, HostMessages > Hosts = new Dictionary<string, HostMessages>();
}