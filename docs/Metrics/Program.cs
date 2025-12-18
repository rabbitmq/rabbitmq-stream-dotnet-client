// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace Metrics;

using System.Net;
using System.Threading;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Metrics;
using RabbitMQ.Stream.Client.Reliable;

class Program
{
    static readonly CancellationTokenSource s_cancel = new();
    static async Task Main()
    {
        // Configure OTEL console exporter to collect metrics from the client and print to console
        // Read OTEL docs for more information:
        // https://opentelemetry.io/docs/languages/dotnet/metrics/getting-started-console/
        using var meterProvider = Sdk.CreateMeterProviderBuilder()
        .AddMeter(StreamMetricsConstants.Name)
        .AddConsoleExporter()
        .Build();

        // Configure the stream to connect to localhost. Adjust if your server is running on a different host.
        var config = new StreamSystemConfig()
        {
            Heartbeat = TimeSpan.Zero,
            Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) },
        };
        var system = await StreamSystem.Create(config).ConfigureAwait(false);

        // Create the stream to use for the producer and consumer.
        await system.CreateStream(new StreamSpec("my-stream")).ConfigureAwait(false);

        // Run the producer and consumer tasks.
        var producerTask = RunProducer(system, s_cancel.Token);
        var consumerTask = RunConsumer(system, s_cancel.Token);

        Console.WriteLine("Application started.");
        Console.WriteLine("Press the ENTER key to cancel...\n");
        var cancelTask = Task.Run(() =>
        {
            while (Console.ReadKey().Key != ConsoleKey.Enter)
            {
                Console.WriteLine("Press the ENTER key to cancel...");
            }

            Console.WriteLine("\nENTER key pressed: cancelling application.\n");
            s_cancel.Cancel();
        });

        await Task.WhenAny(producerTask, consumerTask, cancelTask).ConfigureAwait(false);
        if (cancelTask.IsCanceled)
        {
            // re-await the producer and consumer tasks to ensure they are closed and to catch any exceptions
            await Task.WhenAll(producerTask, consumerTask).ConfigureAwait(false);
        }

        // Delete the stream and close the system. Good practice to clean up after the example is done.
        await system.DeleteStream("my-stream").ConfigureAwait(false);
        await system.Close().ConfigureAwait(false);
        Console.WriteLine("Application closed");
    }

    static async Task RunProducer(StreamSystem system, CancellationToken cancellationToken)
    {
        var producer = await Producer.Create(new ProducerConfig(system, "my-stream")).ConfigureAwait(false);
        var count = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            await producer.Send(new Message(System.Text.Encoding.UTF8.GetBytes($"Message {count}"))).ConfigureAwait(false);
            count++;
            if (count % 100 == 0)
            {
                await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            }
        }

        await producer.Close().ConfigureAwait(false);
    }

    static async Task RunConsumer(StreamSystem system, CancellationToken cancellationToken)
    {
        var count = 0;
        var c = new ConsumerConfig(system, "my-stream")
        {
            MessageHandler = async (stream, consumer, context, message) =>
            {
                count += 1;
                if (count % 1_000 == 0)
                {
                    Console.Write(".");
                }

                await Task.CompletedTask.ConfigureAwait(false);
            }
        };
        var consumer = await Consumer.Create(c).ConfigureAwait(false);
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
        }

        await consumer.Close().ConfigureAwait(false);
    }
}
