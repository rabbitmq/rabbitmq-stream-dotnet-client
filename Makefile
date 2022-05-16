all: format test

format:
	dotnet format

build:
	dotnet build

test: build
	dotnet test Tests/Tests.csproj --no-build --logger "console;verbosity=detailed" /p:AltCover=true

rabbitmq-server:
	docker run -it --rm --name rabbitmq-stream-docker \
		-p 5552:5552 -p 5672:5672 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		--pull always \
		pivotalrabbitmq/rabbitmq-stream

## Simulate the CI tests
## in GitHub Action the number of the CPU is limited
run-test-in-docker:
	docker stop dotnet-test || true
	docker build -t stream-dotnet-test -f Docker/Dockerfile  . && \
	docker run -d --rm --name dotnet-test -v $(shell pwd):/source --cpuset-cpus="1" stream-dotnet-test && \
    docker exec -it dotnet-test /bin/sh -c "cd /source && make" || true  
