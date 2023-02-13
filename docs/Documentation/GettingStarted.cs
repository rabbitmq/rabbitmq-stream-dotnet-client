// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Text;
// tag::sample-imports[]
using System.Net;
using Microsoft.Extensions.Logging; // <1>
using RabbitMQ.Stream.Client; // <2>
using RabbitMQ.Stream.Client.Reliable; // <3>
// end::sample-imports[]


namespace Documentation;

public class GettingStarted
{
    public static async Task Start()
    {
        // The Logger is not mandatory but it is very useful to understand what is going on.
        // The logger is a Microsoft.Extensions.Logging.ILogger
        // In this example we are using the Microsoft.Extensions.Logging.Console package.
        // Microsoft.Extensions.Logging.Console is NOT shipped with the client.
        // You can use any logger you want.
        // tag::sample-logging[]
        var factory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });

        // Define the logger for the StreamSystem and the Producer/Consumer
        var producerLogger = factory.CreateLogger<Producer>(); // <1>
        var consumerLogger = factory.CreateLogger<Consumer>(); // <2>
        var streamLogger = factory.CreateLogger<StreamSystem>(); // <3>
        // end::sample-logging[]

        // Create the StreamSystem
        // tag::sample-system[]
        var streamSystem = await StreamSystem.Create( // <1>
            new StreamSystemConfig() // <2>
            {
                UserName = "guest",
                Password = "guest",
                Endpoints = new List<EndPoint>() {new IPEndPoint(IPAddress.Loopback, 5552)}
            },
            streamLogger // <3>
        ).ConfigureAwait(false);

        // Create a stream

        const string StreamName = "my-stream";
        await streamSystem.CreateStream(
            new StreamSpec(StreamName) // <4>
            {
                MaxSegmentSizeBytes = 20_000_000 // <5>
            }).ConfigureAwait(false);
        // end::sample-system[]

        // Create a producer
        // tag::sample-producer[]
        var confirmationTaskCompletionSource = new TaskCompletionSource<int>();
        var confirmationCount = 0;
        const int MessageCount = 100;
        var producer = await Producer.Create( // <1>
                new ProducerConfig(streamSystem, StreamName)
                {
                    ConfirmationHandler = async confirmation => // <2>
                    {
                        Interlocked.Increment(ref confirmationCount);

                        // here you can handle the confirmation
                        switch (confirmation.Status)
                        {
                            case ConfirmationStatus.Confirmed: // <3>
                                // all the messages received here are confirmed
                                if (confirmationCount == MessageCount)
                                {
                                    Console.WriteLine("*********************************");
                                    Console.WriteLine($"All the {MessageCount} messages are confirmed");
                                    Console.WriteLine("*********************************");
                                }

                                break;

                            case ConfirmationStatus.StreamNotAvailable:
                            case ConfirmationStatus.InternalError:
                            case ConfirmationStatus.AccessRefused:
                            case ConfirmationStatus.PreconditionFailed:
                            case ConfirmationStatus.PublisherDoesNotExist:
                            case ConfirmationStatus.UndefinedError:
                            case ConfirmationStatus.ClientTimeoutError:
                                // <4>
                                Console.WriteLine(
                                    $"Message {confirmation.PublishingId} failed with {confirmation.Status}");
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }

                        if (confirmationCount == MessageCount)
                        {
                            confirmationTaskCompletionSource.SetResult(MessageCount);
                        }

                        await Task.CompletedTask.ConfigureAwait(false);
                    }
                },
                producerLogger // <5>
            )
            .ConfigureAwait(false);


        // Send 100 messages
        Console.WriteLine("Starting publishing...");
        for (var i = 0; i < MessageCount; i++)
        {
            await producer.Send( // <6>
                new Message(Encoding.ASCII.GetBytes($"{i}"))
            ).ConfigureAwait(false);
        }


        confirmationTaskCompletionSource.Task.Wait(); // <7>
        await producer.Close().ConfigureAwait(false); // <8>
        // end::sample-producer[]

        var consumerTaskCompletionSource = new TaskCompletionSource<int>();
        var consumerCount = 0;
        // Create a consumer
        // tag::sample-consumer[]
        Console.WriteLine("Starting consuming...");
        var consumer = await Consumer.Create( // <1>
                new ConsumerConfig(streamSystem, StreamName)
                {
                    OffsetSpec = new OffsetTypeFirst(), // <2>
                    MessageHandler = async (sourceStream, consumer, messageContext, message) => // <3>
                    {
                        if (Interlocked.Increment(ref consumerCount) == MessageCount)
                        {
                            Console.WriteLine("*********************************");
                            Console.WriteLine($"All the {MessageCount} messages are received");
                            Console.WriteLine("*********************************");
                            consumerTaskCompletionSource.SetResult(MessageCount);
                        }
                        await Task.CompletedTask.ConfigureAwait(false);
                    }
                },
                consumerLogger // <4>
            )
            .ConfigureAwait(false);
        consumerTaskCompletionSource.Task.Wait(); // <5>
        await consumer.Close().ConfigureAwait(false); // <6>
        // end::sample-consumer[]

        Console.WriteLine("Press any key to exit");
        Console.ReadKey();
        //tag::sample-close[]
        await streamSystem.DeleteStream(StreamName).ConfigureAwait(false); // <1>
        await streamSystem.Close().ConfigureAwait(false); // <2>
        //end::sample-close[]
    }
}
