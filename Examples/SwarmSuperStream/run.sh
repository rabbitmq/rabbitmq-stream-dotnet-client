#!/bin/sh
docker stack rm superstack
docker build ./src/SuperStreamClients -t swarmsuperstreamclient
docker build ./rabbitmq -t swarmsuperstreamrabbitmq
docker stack deploy -c docker-compose.yml superstack
docker service logs superstack_analytics -f