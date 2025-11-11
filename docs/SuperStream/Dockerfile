FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["../../../rabbitmq/rabbitmq-stream-dotnet-client/docs/SuperStream/SuperStream.csproj", "../../../rabbitmq/rabbitmq-stream-dotnet-client/docs/SuperStream/"]
COPY ["../../../rabbitmq/rabbitmq-stream-dotnet-client/RabbitMQ.Stream.Client/RabbitMQ.Stream.Client.csproj", "../../../rabbitmq/rabbitmq-stream-dotnet-client/RabbitMQ.Stream.Client/"]
RUN dotnet restore "../../../rabbitmq/rabbitmq-stream-dotnet-client/docs/SuperStream/SuperStream.csproj"
COPY . .
WORKDIR "/src/../../../rabbitmq/rabbitmq-stream-dotnet-client/docs/SuperStream"
RUN dotnet build "./SuperStream.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./SuperStream.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SuperStream.dll"]
