// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace ReliableClient;

public class BestPracticesClient
{
    public record Config
    {
        public string? Host { get; set; } = "localhost";
        public int Port { get; set; } = 5552;
        public string? Username { get; set; } = "guest";
        public string? Password { get; set; } = "guest";

        public string? StreamName { get; set; } = "DotNetClientTest";
        public bool LoadBalancer { get; set; } = false;
        public bool SuperStream { get; set; } = false;
        public int Streams { get; set; } = 1;
        public int Producers { get; set; } = 9;
        public byte ProducersPerConnection { get; set; } = 7;
        public int MessagesPerProducer { get; set; } = 5_000_000;
        public int Consumers { get; set; } = 9;
        public byte ConsumersPerConnection { get; set; } = 8;

        public int DelayDuringSendMs { get; set; } = 0;
    }

    public static async Task Start(Config config)
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddLogging(builder => builder
            .AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "[HH:mm:ss] ";
                options.ColorBehavior = LoggerColorBehavior.Default;
            })
            .AddFilter(level => level >= LogLevel.Information)
        );
        var loggerFactory = serviceCollection.BuildServiceProvider()
            .GetService<ILoggerFactory>();


        if (loggerFactory != null)
        {
            var lp = loggerFactory.CreateLogger<Producer>();
            var lc = loggerFactory.CreateLogger<Consumer>();
            var ls = loggerFactory.CreateLogger<StreamSystem>();

            var ep = new IPEndPoint(IPAddress.Loopback, config.Port);

            if (config.Host != "localhost")
            {
                switch (Uri.CheckHostName(config.Host))
                {
                    case UriHostNameType.IPv4:
                        if (config.Host != null) ep = new IPEndPoint(IPAddress.Parse(config.Host), config.Port);
                        break;
                    case UriHostNameType.Dns:
                        if (config.Host != null)
                        {
                            var addresses = await Dns.GetHostAddressesAsync(config.Host).ConfigureAwait(false);
                            ep = new IPEndPoint(addresses[0], config.Port);
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            var streamConf = new StreamSystemConfig()
            {
                UserName = config.Username,
                Password = config.Password,
                Endpoints = new List<EndPoint>() { ep },
                ConnectionPoolConfig = new ConnectionPoolConfig()
                {
                    ProducersPerConnection = config.ProducersPerConnection,
                    ConsumersPerConnection = config.ConsumersPerConnection,
                }
            };


            if (config.LoadBalancer)
            {
                var resolver = new AddressResolver(ep);
                streamConf = new StreamSystemConfig()
                {
                    AddressResolver = resolver,
                    UserName = config.Username,
                    Password = config.Password,
                    ConnectionPoolConfig = new ConnectionPoolConfig()
                    {
                        ProducersPerConnection = config.ProducersPerConnection,
                        ConsumersPerConnection = config.ConsumersPerConnection,
                    },
                    Endpoints = new List<EndPoint>() { resolver.EndPoint }
                };
            }


            var system = await StreamSystem.Create(streamConf, ls).ConfigureAwait(false);
            var streamsList = new List<string>();
            if (config.SuperStream)
            {
                if (config.StreamName != null) streamsList.Add(config.StreamName);
            }
            else
            {
                for (var i = 0; i < config.Streams; i++)
                {
                    streamsList.Add($"{config.StreamName}-{i}");
                }
            }


            var totalConfirmed = 0;
            var totalError = 0;
            var totalConsumed = 0;
            var totalSent = 0;
            var isRunning = true;

            _ = Task.Run(() =>
            {
                while (isRunning)
                {
                    Console.WriteLine(
                        $"When: {DateTime.Now}, " +
                        $"Tr {System.Diagnostics.Process.GetCurrentProcess().Threads.Count}, " +
                        $"Sent: {totalSent:#,##0.00}, " +
                        $"Conf: {totalConfirmed:#,##0.00}, " +
                        $"Error: {totalError:#,##0.00}, " +
                        $"Total: {(totalConfirmed + totalError):#,##0.00}, " +
                        $"Consumed: {totalConsumed:#,##0.00}, " +
                        $"Sent per stream: {totalSent / streamsList.Count}");
                    Thread.Sleep(5000);
                }
            });
            List<Consumer> consumersList = new();
            List<Producer> producersList = new();
            var obj = new object();
            if (config.SuperStream)
            {
                if (await system.SuperStreamExists(streamsList[0]).ConfigureAwait(false))
                {
                    await system.DeleteSuperStream(streamsList[0]).ConfigureAwait(false);
                }


                await system.CreateSuperStream(new PartitionsSuperStreamSpec(streamsList[0], config.Streams))
                    .ConfigureAwait(false);
            }


            foreach (var stream in streamsList)
            {
                if (!config.SuperStream)
                {
                    if (await system.StreamExists(stream).ConfigureAwait(false))
                    {
                        await system.DeleteStream(stream).ConfigureAwait(false);
                    }

                    await system.CreateStream(new StreamSpec(stream) { MaxLengthBytes = 30_000_000_000, })
                        .ConfigureAwait(false);
                    await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
                }

                for (var z = 0; z < config.Consumers; z++)
                {
                    var conf = new ConsumerConfig(system, stream)
                    {
                        OffsetSpec = new OffsetTypeLast(),
                        IsSuperStream = config.SuperStream,
                        IsSingleActiveConsumer = config.SuperStream,
                        Reference = "myApp", // needed for the Single Active Consumer or fot the store offset 
                        // can help to identify the consumer on the logs and RabbitMQ Management
                        Identifier = $"my_consumer_{z}",
                        InitialCredits = 10,
                        MessageHandler = async (source, consumer, ctx, _) =>
                        {
                            if (totalConsumed % 10_000 == 0)
                            {
                                // don't store the offset every time, it could be a performance issue
                                // store the offset every 1_000/5_000/10_000 messages
                                await consumer.StoreOffset(ctx.Offset).ConfigureAwait(false);
                            }

                            Interlocked.Increment(ref totalConsumed);
                        },
                    };

                    // This is the callback that will be called when the consumer status changes
                    // DON'T PUT ANY BLOCKING CODE HERE
                    conf.StatusChanged += (status) =>
                    {
                        var streamInfo = status.Partition is not null
                            ? $" Partition {status.Partition} of super stream: {status.Stream}"
                            : $"Stream: {status.Stream}";

                        lc.LogInformation(
                            "Consumer: {Id} - status changed from: {From} to: {To} reason: {Reason}  {Info}",
                            status.Identifier, status.From, status.To, status.Reason, streamInfo);
                    };
                    consumersList.Add(
                        await Consumer.Create(conf, lc).ConfigureAwait(false));
                }

                async Task MaybeSend(Producer producer, Message message, ManualResetEvent publishEvent)
                {
                    publishEvent.WaitOne();
                    await producer.Send(message).ConfigureAwait(false);
                }

                // this example is meant to show how to use the producer and consumer
                // Create too many tasks for the producers and consumers is not a good idea
                for (var z = 0; z < config.Producers; z++)
                {
                    var z1 = z;
                    _ = Task.Run(async () =>
                    {
                        // the list of unconfirmed messages in case of error or disconnection
                        // This example is only for the example, in a real scenario you should handle the unconfirmed messages
                        // since the list could grow event the publishEvent should avoid it.
                        var unconfirmedMessages = new ConcurrentBag<Message>();
                        // the event to wait for the producer to be ready to send
                        // in case of disconnection the event will be reset
                        var publishEvent = new ManualResetEvent(false);
                        var producerConfig = new ProducerConfig(system, stream)
                        {
                            Identifier = $"my_producer_{z1}",
                            SuperStreamConfig = new SuperStreamConfig()
                            {
                                Enabled = config.SuperStream, Routing = msg => msg.Properties.MessageId.ToString(),
                            },
                            ConfirmationHandler = confirmation =>
                            {
                                // Add the unconfirmed messages to the list in case of error
                                if (confirmation.Status != ConfirmationStatus.Confirmed)
                                {
                                    confirmation.Messages.ForEach(m => { unconfirmedMessages.Add(m); });
                                    Interlocked.Add(ref totalError, confirmation.Messages.Count);
                                    return Task.CompletedTask;
                                }

                                Interlocked.Add(ref totalConfirmed, confirmation.Messages.Count);
                                return Task.CompletedTask;
                            },
                        };

                        // Like the consumer don't put any blocking code here
                        producerConfig.StatusChanged += (status) =>
                        {
                            var streamInfo = status.Partition is not null
                                ? $" Partition {status.Partition} of super stream: {status.Stream}"
                                : $"Stream: {status.Stream}";

                            // just log the status change
                            lp.LogInformation(
                                "Consumer: {Id} - status changed from: {From} to: {To} reason: {Reason}  {Info}",
                                status.Identifier, status.From, status.To, status.Reason, streamInfo);

                            // in case of disconnection the event will be reset
                            // in case of reconnection the event will be set so the producer can send messages
                            // It is important to use the ManualReset to avoid to send messages before the producer is ready
                            if (status.To == ReliableEntityStatus.Open)
                            {
                                publishEvent.Set();
                            }
                            else
                            {
                                publishEvent.Reset();
                            }
                        };
                        var producer = await Producer.Create(producerConfig, lp).ConfigureAwait(false);
                        lock (obj)
                        {
                            producersList.Add(producer);
                        }

                        for (var i = 0; i < config.MessagesPerProducer; i++)
                        {
                            if (!unconfirmedMessages.IsEmpty)
                            {
                                // checks if there are unconfirmed messages and send them
                                var msgs = unconfirmedMessages.ToArray();
                                unconfirmedMessages.Clear();
                                foreach (var msg in msgs)
                                {
                                    await MaybeSend(producer, msg, publishEvent).ConfigureAwait(false);
                                    Interlocked.Increment(ref totalSent);
                                }
                            }

                            var message = new Message(Encoding.Default.GetBytes("hello"))
                            {
                                Properties = new Properties() { MessageId = $"hello{i}" }
                            };
                            await MaybeSend(producer, message, publishEvent).ConfigureAwait(false);
                            // You don't need this it is only for the example
                            await Task.Delay(config.DelayDuringSendMs).ConfigureAwait(false);
                            Interlocked.Increment(ref totalSent);
                        }
                    });
                }
            }


            Console.WriteLine("Press any key to close all the consumers");
            Console.ReadKey();
            isRunning = false;
            Console.WriteLine("closing the producers ..... ");
            producersList.ForEach(async p => await p.Close().ConfigureAwait(false));
            Console.WriteLine("closing the consumers ..... ");
            consumersList.ForEach(async c => await c.Close().ConfigureAwait(false));

            Console.WriteLine("Press any key to close all");
            Console.ReadKey();
        }

        Console.WriteLine("Closed all the consumers and producers");
    }
}
