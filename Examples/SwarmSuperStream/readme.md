# Super Streams

This demo shows multiple consumers running, with a producer sending messages for a number of different "customers".

The messages for each customer will be processed in order on the same host. If a host goes down, or the number of consumers is increased, the host processing the messages for a consumer will change.

By default there are 30 stream partitions configured, 10 customer numbers, and 2 consumers.

Each consumer writes the messages it receives to a single analytics API service that then shows what consumers have handled what.

The log output of the analytics service shows for each customer, what hosts have handled its messages, and the last 10 messages handled by the host.

## Configuration

The defaults for the configuration can be seen in RabbitMqStreamOptions.cs

```
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
```

These can be set from Environment Variables, appsettings.json, or inline code.

From docker compose environment variables
```
  producer:
    image: swarmsuperstreamclient
    depends_on:
      - rabbitmq
    environment:
      - SwarmSuperStreams__Consumer=false
      - SwarmSuperStreams__Producer=true
      - SwarmSuperStreams__Analytics=false   
      - SwarmSuperStreams__NumberOfCustomers=10
      - Logging__LogLevel__Default=Information
      - DOTNET_ENVIRONMENT=Production
    deploy:
```

App Settings
```
{
  "SwarmSuperStreams": {
    "StreamName": "SwarmSuperStream",
    "NumberOfCustomers": 10,
    "HostName": "localhost"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Warning",
      "Microsoft.AspNetCore": "Warning",
      "SuperStreamClients": "Warning",
      "SuperStreamClients.Analytics": "Information"
    }
  }
}
```

Inline code
```
builder.Services.AddSwarmSuperStream(
    builder.Configuration.GetSection(RabbitMqStreamOptions.Name),
    options =>
    {
        options.ConsumerHandleDelayMin = 100;
    });
```

[TODO]

### Example Config Chamges

#### Change Number of Customers

[TODO]

## Features to add
- Web GUI for showing stats / analytics


## Run In Swarm
This demo uses docker swarm to show scaling of consumers up and down.

### Pre Reqs

Have docker swarm running 

```
docker swarm init
```

### Run
Set permissions and run

```
sudo chmod +x ./run.sh
./run.sh
````

> If running the run.sh again and there is an error creating the stack. Wait a few seconds and then re run the run.sh command, as it may be taking a while to remove the stack before it is recreated.

### Configuring 
Scale the number of consumers up with:

```
docker service scale superstack_consumer=5
```

Or down to 1 with:

```
docker service scale superstack_consumer=1
```



### View logs
By default, the analytics service logs are shown after the service is running (it will take a little while for rabbitmq to come online before the first output is shown). 

Logs for producer and consumer can be viewed through another terminal.

```
docker service logs superstack_consumer -f
docker service logs superstack_producer -f
```

### Issues



## Run Locally

The demo can also run in a single app with a local rabbitmq instance. Configuration allow turning consumer/producer/analytics on or off.

Build Pre Configured SuperStream Docker Image
```
docker build ./rabbitmq -t swarmsuperstreamrabbitmq
```

Run Rabbit Docker Image
```
docker run \
--name swarmsuperstreamrabbitmq1 \
-p 4369:4369 -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 -p 15691:15691 -p 15692:15692 -p 25672:25672 -p 5552:5552 \
-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
swarmsuperstreamrabbitmq
```

Run App

```
dotnet run --project src/SuperStreamClients 
```
