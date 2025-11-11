FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["docs/SuperStream/SuperStream.csproj", "docs/SuperStream/"]
COPY ["RabbitMQ.Stream.Client/RabbitMQ.Stream.Client.csproj", "RabbitMQ.Stream.Client/"]
RUN dotnet restore "docs/SuperStream/SuperStream.csproj"
COPY . .
WORKDIR "/src/docs/SuperStream"
RUN ls -la
RUN dotnet build "/src/docs/SuperStream/SuperStream.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN  ls -la
WORKDIR "/src/docs/SuperStream"
RUN dotnet publish "/src/docs/SuperStream/SuperStream.csproj" -c $BUILD_CONFIGURATION --framework=net9.0 -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SuperStream.dll"]
