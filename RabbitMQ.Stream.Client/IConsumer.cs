// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public interface IConsumer
{
    public Task StoreOffset(ulong offset);
    public Task<ResponseCode> Close();
    public void Dispose();
}

public record IConsumerConfig : INamedEntity
{
    // ClientProvidedName is used to identify TCP connection name.
    public string ClientProvidedName { get; set; } = "dotnet-stream-consumer";

    public string Reference { get; set; }
}
