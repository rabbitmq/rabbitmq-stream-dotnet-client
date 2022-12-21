namespace SuperStreamClients.Analytics;

public class AnalyticsInfo
{
    public List<string> Sources = new List<string>();
    public Dictionary<string, HostMessages > Hosts = new Dictionary<string, HostMessages>();
}

public class HostMessages
{
    public string Host{get;set;}
    public TimeOnly TimeSeen{get;set;}
    public  List<ReceivedCustomerMessage> Messages {get;set;} = new List<ReceivedCustomerMessage>(); 

}
