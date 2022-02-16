@echo off
set DOTNET_CLI_TELEMETRY_OPTOUT=1
dotnet restore --verbosity=normal .\rabbitmq-stream-dotnet-client.sln
dotnet build --verbosity=normal .\rabbitmq-stream-dotnet-client.sln
