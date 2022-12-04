// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
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
    // StoredOffsetSpec configuration it is needed to keep the offset spec.
    // since the offset can be decided from the ConsumerConfig.OffsetSpec.
    // and from ConsumerConfig.ConsumerUpdateListener.
    // needed also See also consumer:MaybeDispatch/1.
    // It is not public because it is not needed for the user.
    internal IOffsetType StoredOffsetSpec { get; set; }

    // ClientProvidedName is used to identify TCP connection name.
    public string ClientProvidedName { get; set; } = "dotnet-stream-raw-consumer";

    // SingleActiveConsumer is used to indicate that there is only one consumer active for the stream.
    // given a consumer reference. 
    // Consumer Reference can't be null or Empty.
    public bool IsSingleActiveConsumer { get; set; } = false;

    // config.ConsumerUpdateListener is the callback for when the consumer is updated due
    // to single active consumer. 
    // return IOffsetType to indicate the offset to be used for the next consumption.
    // if the ConsumerUpdateListener==null the OffsetSpec will be used.
    public Func<string, string, bool, Task<IOffsetType>> ConsumerUpdateListener { get; set; } = null;

    public string Reference { get; set; }

    public Func<string, Task> ConnectionClosedHandler { get; set; }
}
