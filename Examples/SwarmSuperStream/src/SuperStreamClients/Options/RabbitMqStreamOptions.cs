namespace SuperStreamClients;

public class RabbitMqStreamOptions
{
    public static string Name = "SwarmSuperStreams";
    public string HostName { get; set; } = "rabbitmq";
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public int StreamPort { get; set; } = 5552;
    public string StreamName { get; set; } = "SwarmSuperStream";
    public int RandomSeed { get; set; } = 5432;
    public int ProducerMessageSendLimit { get; set; } = int.MaxValue;
    public int NumberOfCustomers { get; set; } = 100;
    public int ProducerSendDelayMin { get; set; } = 100;
    public int ProducerSendDelayMax { get; set; } = 1000;
    public string ConsumerAppReference { get; set; } = "SwarmSuperStreamApp";
    public int ConsumerHandleDelayMin { get; set; } = 100;
    public int ConsumerHandleDelayMax { get; set; } = 1000;
    public int MaxMessagesToShowInAnalytics {get;set;} = 10;
    public bool Consumer {get;set;} = true;
    public bool Producer {get;set;} = true;
    public bool Analytics {get;set;} = true;
    public string AnalyticsApi{get;set;}="http://localhost:5070";
}