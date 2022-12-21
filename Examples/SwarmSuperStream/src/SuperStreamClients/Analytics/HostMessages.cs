namespace SuperStreamClients.Analytics;

public class HostMessages
{
    public string Host{get;set;}
    public TimeOnly TimeSeen{get;set;}
    public  List<ReceivedCustomerMessage> Messages {get;set;} = new List<ReceivedCustomerMessage>(); 

}
