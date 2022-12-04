// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client;

// <summary>
// Producer interface for sending messages to a stream.
// There are different types of producers:
// - Standard producer
// - Super-Stream producer
// </summary>

public interface IProducer
{
    /// <summary>
    /// Send the message to the stream in asynchronous mode.
    /// The client will aggregate messages and send them in batches.
    /// The batch size is configurable. See IProducerConfig.BatchSize.
    /// </summary>
    /// <param name="publishingId">Publishing id</param>
    /// <param name="message"> Message </param>
    /// <returns></returns>
    public ValueTask Send(ulong publishingId, Message message);

    /// <summary>
    /// Send the messages in batch to the stream in synchronous mode.
    /// The aggregation is provided by the user.
    /// The client will send the messages in the order they are provided.
    /// </summary>
    /// <param name="messages">Batch messages to send</param>
    /// <returns></returns>
    public ValueTask Send(List<(ulong, Message)> messages);

    /// <summary>
    /// Enable sub-batch feature.
    /// It is needed when you need to sub aggregate the messages and compress them.
    /// For example you can aggregate 100 log messages and compress to reduce the space.
    /// One single publishingId can have multiple sub-batches messages.
    /// See also: https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#sub-entry-batching-and-compression
    /// </summary>
    /// <param name="publishingId"></param>
    /// <param name="subEntryMessages">Messages to aggregate</param>
    /// <param name="compressionType"> Type of compression. By default the client supports GZIP and none</param>
    /// <returns></returns>
    public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType);

    public Task<ResponseCode> Close();

    /// <summary>
    /// Return the last publishing id.
    /// </summary>
    /// <returns></returns>
    public Task<ulong> GetLastPublishingId();

    public bool IsOpen();

    public void Dispose();

    public int MessagesSent { get; }
    public int ConfirmFrames { get; }
    public int IncomingFrames { get; }
    public int PublishCommandsSent { get; }

    public int PendingCount { get; }
}

public record IProducerConfig : INamedEntity
{
    public string Reference { get; set; }
    public int MaxInFlight { get; set; } = 1000;
    public string ClientProvidedName { get; set; } = "dotnet-stream-raw-producer";

    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Number of the messages sent for each frame-send.
    /// High values can increase the throughput.
    /// Low values can reduce the messages latency.
    /// Default value is 100.
    /// </summary>
    public int MessagesBufferSize { get; set; } = 100;
}
