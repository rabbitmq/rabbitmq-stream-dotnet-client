// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client.Reliable;

public record DeduplicatingProducerConfig : ProducerConfig
{
    public DeduplicatingProducerConfig(StreamSystem streamSystem, string stream, string reference) : base(streamSystem,
        stream)
    {
        if (string.IsNullOrWhiteSpace(reference))
            throw new ArgumentException("Reference cannot be null or empty", nameof(reference));
        _reference = reference;
    }
}

// DeduplicationProducer is a wrapper around the Producer class
// to handle the deduplication of the messages.
// The deduplication is enabled by setting the reference in the DeduplicationProducerConfig 
// and it is mandatory to set the reference.
// This class it to use in an easy way the deduplication feature.
// the low level API is the RawProducer class, this class sets the right parameters to enable the deduplication
// The only api is `Send(ulong publishing, Message message)`. In this case the user has to manage the sequence
// to decide deduplication or not.
// The best way to handle the deduplication is to use a single thread avoiding the id overlaps.

public class DeduplicatingProducer
{
    private Producer _producer = null!;

    public static async Task<DeduplicatingProducer> Create(DeduplicatingProducerConfig producerConfig,
        ILogger<Producer> logger = null)
    {
        var x = new DeduplicatingProducer()
        {
            _producer = await Producer
                .Create(
                    new ProducerConfig(producerConfig.StreamSystem, producerConfig.Stream)
                    {
                        _reference = producerConfig.Reference,
                        ConfirmationHandler = producerConfig.ConfirmationHandler,
                        ReconnectStrategy = producerConfig.ReconnectStrategy,
                        ClientProvidedName = producerConfig.ClientProvidedName,
                        MaxInFlight = producerConfig.MaxInFlight,
                        MessagesBufferSize = producerConfig.MessagesBufferSize,
                        TimeoutMessageAfter = producerConfig.TimeoutMessageAfter,
                    }, logger)
                .ConfigureAwait(false)
        };
        return x;
    }

    private DeduplicatingProducer()
    {
    }

    // Send a message with a specific publishing id
    // the publishing id is used to deduplicate the messages
    // the publishing id must be unique and incremental. It can accept gaps the important is to be incremental
    public async ValueTask Send(ulong publishing, Message message)
    {
        await _producer.SendInternal(publishing, message).ConfigureAwait(false);
    }

    public async Task Close()
    {
        await _producer.Close().ConfigureAwait(false);
    }

    public bool IsOpen()
    {
        return _producer.IsOpen();
    }

    // Get the last publishing id from the producer/reference
    // this is useful to know the last id used to deduplicate the messages
    // so it is possible to restart the producer with the last id
    public async Task<ulong> GetLastPublishedId()
    {
        return await _producer.GetLastPublishingId().ConfigureAwait(false);
    }
}
