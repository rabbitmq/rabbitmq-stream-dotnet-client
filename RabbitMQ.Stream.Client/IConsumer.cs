﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public interface IConsumer : IClosable
{
    public Task StoreOffset(ulong offset);
    public void Dispose();

    public ConsumerInfo Info { get; }
}

public record IConsumerConfig : EntityCommonConfig, INamedEntity
{
    private ushort _initialCredits = Consts.ConsumerInitialCredits;

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

    public ConsumerFilter ConsumerFilter { get; set; } = null;

    // InitialCredits is the initial credits to be used for the consumer.
    // if the InitialCredits is not set, the default value will be 2.
    // It is the number of the chunks that the consumer will receive at beginning.
    // A high value can increase the throughput but could increase the memory usage and server-side CPU usage.
    // The RawConsumer uses this value to create the Channel buffer so all the chunks will be stored in the buffer memory.
    // The default value it is usually a good value.
    public ushort InitialCredits
    {
        get => _initialCredits;
        set
        {
            if (value < 1)
            {
                throw new ArgumentException(
                    $"InitialCredits must be greater than 0. Default value is {Consts.ConsumerInitialCredits}.");
            }

            _initialCredits = value;
        }
    }

    // enables the check of the crc on the delivery.
    // the server will send the crc for each chunk and the client will check it.
    // It is not enabled by default because it is could reduce the performance.
    public ICrc32 Crc32 { get; set; } = null;
}

public class ConsumerInfo : Info
{
    public string Reference { get; }

    public ConsumerInfo(string stream, string reference) : base(stream)
    {
        Reference = reference;
    }

    public override string ToString()
    {
        return $"{base.ToString()}, Reference: {Reference}";
    }
}
