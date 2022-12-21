# Super Streams

## Run Locally

Build Docker Image
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
dotnet run --project SuperStreamClients 
```

