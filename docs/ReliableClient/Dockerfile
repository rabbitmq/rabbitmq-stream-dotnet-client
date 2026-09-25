# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

# The repository relies on root-level MSBuild files (Directory.Build.props,
# Directory.Packages.props, NuGet.Config, global.json) for central package
# management, so the whole build context is copied rather than just the
# two project folders.
COPY . .

RUN dotnet restore docs/ReliableClient/ReliableClient.csproj

RUN dotnet publish docs/ReliableClient/ReliableClient.csproj \
    -c Release \
    -f net9.0 \
    -p:TreatWarningsAsErrors=false \
    -o /out \
    --no-restore

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:9.0

WORKDIR /app
COPY --from=build /out .

# RabbitMQ Streaming connection settings
ENV HOST=localhost
ENV PORT=5552
ENV USERNAME=guest
ENV PASSWORD=guest
ENV LOAD_BALANCER=false

# Stream / super-stream topology
ENV STREAM_NAME=DotNetClientTest
ENV SUPER_STREAM=false
ENV STREAMS=1

# Producer settings
ENV PRODUCERS=9
ENV PRODUCERS_PER_CONNECTION=7
ENV MESSAGES_PER_PRODUCER=5000000
ENV DELAY_DURING_SEND_MS=0
ENV ENABLE_RESENDING=false

# Consumer settings
ENV CONSUMERS=9
ENV CONSUMERS_PER_CONNECTION=8

ENTRYPOINT ["dotnet", "ReliableClient.dll"]
