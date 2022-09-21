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
// - Reliable producer
// </summary>

public interface IProducer
{
    public ValueTask Send(ulong publishingId, Message message);
    public ValueTask BatchSend(List<(ulong, Message)> messages);

    public ValueTask Send(ulong publishingId, List<Message> subEntryMessages, CompressionType compressionType);

    public Task<ResponseCode> Close();

    public Task<ulong> GetLastPublishingId();

    public bool IsOpen();

    public void Dispose();

    public int MessagesSent { get; }
    public int ConfirmFrames { get; }
    public int IncomingFrames { get; }
    public int PublishCommandsSent { get; }

    public int PendingCount { get; }
}
