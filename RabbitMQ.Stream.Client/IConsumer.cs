// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

public interface ISuperStreamConsumer : IConsumer
{
    public Task ReconnectPartition(StreamInfo streamInfo);
}

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

    // It is enabled by default. You can disable it by setting it to null.
    // It is recommended to keep it enabled. Disable it only for performance reasons.
    public ICrc32 Crc32 { get; set; } = new StreamCrc32();

    public FlowControl FlowControl { get; set; } = new FlowControl();
}

public class ConsumerInfo(string stream, string reference, string identifier, List<string> partitions)
    : Info(stream,
        identifier, partitions)
{
    public string Reference { get; } = reference;

    public override string ToString()
    {
        var partitions = Partitions ?? [];
        return
            $"ConsumerInfo(Stream={Stream}, Reference={Reference}, Identifier={Identifier}, Partitions={string.Join(",", partitions)})";
    }
}

public enum ConsumerFlowStrategy
{
    /// <summary>
    /// Request credits before parsing the chunk.
    /// Default strategy. The best for performance.
    /// </summary>
    CreditsBeforeParseChunk,

    /// <summary>
    /// Request credits after parsing the chunk.
    /// It can be useful if the parsing is expensive and you want to avoid requesting credits too early.
    /// Useful for slow processing of chunks.
    /// </summary>
    CreditsAfterParseChunk,

    /// <summary>
    /// The user manually requests credits with <see cref="RawConsumer.Credits"/>
    /// to request credits and
    /// have more chunks to process.
    /// </summary>
    ConsumerCredits
}

/// <summary>
/// FlowControl is used to control the flow of the consumer.
/// See <see cref="ConsumerFlowStrategy"/> for the available strategies.
/// Open for future extensions.
/// </summary>ra
public class FlowControl
{
    public ConsumerFlowStrategy Strategy { get; set; } = ConsumerFlowStrategy.CreditsBeforeParseChunk;

}
