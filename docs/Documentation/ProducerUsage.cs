﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace Documentation;

public class ProducerUsage
{
    public static async Task CreateProducer()
    {
        // tag::producer-creation[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var producer = await Producer.Create( // <1>
            new ProducerConfig(
                streamSystem,
                "my-stream") // <2>
        ).ConfigureAwait(false);

        await producer.Close().ConfigureAwait(false); // <3>
        await streamSystem.Close().ConfigureAwait(false);
        // end::producer-creation[]
    }


    public static async Task ProducerPublish()
    {
        // tag::producer-publish[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var producer = await Producer.Create(
            new ProducerConfig(
                streamSystem,
                "my-stream")
            {
                ConfirmationHandler = async confirmation => // <5>
                {
                    switch (confirmation.Status)
                    {
                        case ConfirmationStatus.Confirmed:
                            Console.WriteLine("Message confirmed");
                            break;
                        case ConfirmationStatus.ClientTimeoutError:
                        case ConfirmationStatus.StreamNotAvailable:
                        case ConfirmationStatus.InternalError:
                        case ConfirmationStatus.AccessRefused:
                        case ConfirmationStatus.PreconditionFailed:
                        case ConfirmationStatus.PublisherDoesNotExist:
                        case ConfirmationStatus.UndefinedError:
                            Console.WriteLine("Message not confirmed with error: {0}", confirmation.Status);
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }


                    await Task.CompletedTask.ConfigureAwait(false);
                }
            }
        ).ConfigureAwait(false);

        var message = new Message(Encoding.UTF8.GetBytes("hello")); // <1>
        await producer.Send(message).ConfigureAwait(false); // <2>
        var list = new List<Message> {message};
        await producer.Send(list).ConfigureAwait(false); // <3>
        await producer.Send(list, CompressionType.Gzip).ConfigureAwait(false); // <4>

        await producer.Close().ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
        // end::producer-publish[]
    }


    public static async Task ProducerComplexMessage()
    {
        // tag::producer-publish-complex-message[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var producer = await Producer.Create(
            new ProducerConfig(
                streamSystem,
                "my-stream") { }
        ).ConfigureAwait(false);

        var message = new Message(Encoding.UTF8.GetBytes("hello")) // <1>
        {
            ApplicationProperties = new ApplicationProperties() // <2>
            {
                {"key1", "value1"}, {"key2", "value2"}
            },
            Properties = new Properties() // <3>
            {
                MessageId = "message-id",
                CorrelationId = "correlation-id",
                ContentType = "application/json",
                ContentEncoding = "utf-8",
            }
        };

        await producer.Send(message).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
        // end::producer-publish-complex-message[]
    }


    public static async Task Deduplication()
    {
        // tag::deduplication-producer[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var deduplicatingProducer = await DeduplicatingProducer.Create( // <1>
            new DeduplicatingProducerConfig(
                streamSystem,
                "my-stream", "my_producer_reference") { }
        ).ConfigureAwait(false);

        var message = new Message(Encoding.UTF8.GetBytes("hello")); // <2>
        await deduplicatingProducer.Send(1, message).ConfigureAwait(false); // <3>
        await deduplicatingProducer.Send(2, message).ConfigureAwait(false);
        await deduplicatingProducer.Send(3, message).ConfigureAwait(false);

        // deduplication is enabled, so this message will be skipped
        await deduplicatingProducer.Send(1, message).ConfigureAwait(false); // <4>


        await streamSystem.Close().ConfigureAwait(false);
        // end::deduplication-producer[]
    }

    public static async Task DeduplicationLastID()
    {
        // tag::deduplication-queries-last-publishing-id[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var deduplicatingProducer = await DeduplicatingProducer.Create( // <1>
            new DeduplicatingProducerConfig(
                streamSystem,
                "my-stream", "my_producer_reference") { }
        ).ConfigureAwait(false);

        var lastid = await deduplicatingProducer.GetLastPublishedId().ConfigureAwait(false); // <2>
        var message = new Message(Encoding.UTF8.GetBytes("hello"));

        await deduplicatingProducer.Send(lastid + 1, message).ConfigureAwait(false); // <3>
        await deduplicatingProducer.Send(lastid + 2, message).ConfigureAwait(false);
        await deduplicatingProducer.Send(lastid + 3, message).ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
        // end::deduplication-queries-last-publishing-id[]
    }

    public static async Task ProducerSubEntryBatching()
    {
        // tag::producer-sub-entry-batching[]
        var streamSystem = await StreamSystem.Create(
            new StreamSystemConfig()
        ).ConfigureAwait(false);

        var producer = await Producer.Create( // <1>
            new ProducerConfig(
                streamSystem,
                "my-stream") // <2>
        ).ConfigureAwait(false);

        var message = new Message(Encoding.UTF8.GetBytes("hello")); 
        var list = new List<Message> {message, message, message}; // <1>
        await producer.Send(list, CompressionType.Gzip).ConfigureAwait(false); // <2>

        await producer.Close().ConfigureAwait(false);
        await streamSystem.Close().ConfigureAwait(false);
        // end::producer-sub-entry-batching[]
    }
}
