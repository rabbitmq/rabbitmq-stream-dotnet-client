using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Performances;

public class BatchVsBatchSend
{
    private const int TotalMessages = 20_000_000;
    private const int MessageSize = 100;
    private const int AggregateBatchSize = 300;
    private const int ModPrintMessages = 10_000_000;

    public static async Task Start()
    {
        Console.WriteLine("Stream Client Performance Test");
        Console.WriteLine("==============================");
        Console.WriteLine("Client will test Batch vs Batch Send");
        Console.WriteLine("Total Messages: {0}", TotalMessages);
        Console.WriteLine("Message Size: {0}", MessageSize);
        Console.WriteLine("Aggregate Batch Size: {0}", AggregateBatchSize);
        Console.WriteLine("Print Messages each: {0} messages", ModPrintMessages);


        var config = new StreamSystemConfig() {Heartbeat = TimeSpan.Zero};
        var system = await StreamSystem.Create(config);
        await BatchSend(system, await RecreateStream(system, "StandardBatchSend"));
        await StandardProducerSend(await RecreateStream(system, "StandardProducerSendNoBatch"), system);
        await RProducerBatchSend(await RecreateStream(system, "ReliableProducerBatch"), system);
        await RProducerSend(await RecreateStream(system, "ReliableProducerSendNoBatch"), system);
    }

    private static async Task RProducerSend(string stream, StreamSystem system)
    {
        Console.WriteLine("*****Reliable Producer Send No Batch*****");
        var total = 0;
        var confirmed = 0;
        var error = 0;
        var reliableProducer = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
        {
            Stream = stream,
            StreamSystem = system,
            MaxInFlight = 1_000_000,
            ConfirmationHandler = messagesConfirmed =>
            {
                if (messagesConfirmed.Status == ConfirmationStatus.Confirmed)
                {
                    confirmed += messagesConfirmed.Messages.Count;
                }
                else
                {
                    error += messagesConfirmed.Messages.Count;
                }

                if (++total % ModPrintMessages == 0)
                {
                    Console.WriteLine(
                        $"*****Reliable Producer Send No Batch Confirmed: {confirmed} Error: {error}*****");
                }

                return Task.CompletedTask;
            }
        });

        var start = DateTime.Now;
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            var array = new byte[MessageSize];
            await reliableProducer.Send(new Message(array));

            if (i % ModPrintMessages == 0)
            {
                Console.WriteLine($"*****Reliable Producer Send No Batch: {i}");
            }
        }

        Console.WriteLine(
            $"*****Reliable Producer Send No Batch***** send time: {DateTime.Now - start}, messages sent: {TotalMessages}");

        Thread.Sleep(1000);
        await reliableProducer.Close();
    }


    private static async Task RProducerBatchSend(string stream, StreamSystem system)
    {
        Console.WriteLine("*****Reliable Producer Batch Send*****");
        var total = 0;
        var confirmed = 0;
        var error = 0;
        var reliableProducer = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
        {
            Stream = stream,
            StreamSystem = system,
            MaxInFlight = 1_000_000,
            ConfirmationHandler = messagesConfirmed =>
            {
                if (messagesConfirmed.Status == ConfirmationStatus.Confirmed)
                {
                    confirmed += messagesConfirmed.Messages.Count;
                }
                else
                {
                    error += messagesConfirmed.Messages.Count;
                }

                if (++total % ModPrintMessages == 0)
                {
                    Console.WriteLine($"*****Reliable Producer Batch Confirmed: {confirmed}, Error: {error}*****");
                }

                return Task.CompletedTask;
            }
        });

        var messages = new List<Message>();


        var start = DateTime.Now;
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            var array = new byte[MessageSize];
            messages.Add(new Message(array));
            if (i % AggregateBatchSize == 0)
            {
                await reliableProducer.BatchSend(messages);
                messages.Clear();
            }

            if (i % ModPrintMessages == 0)
            {
                Console.WriteLine($"*****Reliable Producer Batch Send: {i}");
            }
        }

        await reliableProducer.BatchSend(messages);
        messages.Clear();

        Console.WriteLine(
            $"*****Reliable Producer Batch Send***** time: {DateTime.Now - start}, messages sent: {TotalMessages}");
        Thread.Sleep(1000);
        await reliableProducer.Close();
    }


    private static async Task StandardProducerSend(string stream, StreamSystem system)
    {
        Console.WriteLine("*****Standard Producer Send*****");
        var confirmed = 0;
        var producer = await system.CreateProducer(new ProducerConfig()
        {
            Stream = stream,
            MaxInFlight = 1_000_000,
            ConfirmHandler = _ =>
            {
                if (++confirmed % ModPrintMessages == 0)
                {
                    Console.WriteLine($"*****Standard Producer Send Confirmed: {confirmed}");
                }
            }
        });

        var start = DateTime.Now;
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            var array = new byte[MessageSize];

            await producer.Send(i, new Message(array));

            if (i % ModPrintMessages == 0)
            {
                Console.WriteLine($"*****Standard Producer: {i}");
            }
        }

        Console.WriteLine(
            $"*****Standard Producer Send***** send time: {DateTime.Now - start}, messages sent: {TotalMessages}");
        Thread.Sleep(1000);
        await producer.Close();
    }

    private static async Task BatchSend(StreamSystem system, string stream)
    {
        Console.WriteLine("*****Standard Batch Send*****");
        var confirmed = 0;
        var producer = await system.CreateProducer(new ProducerConfig()
        {
            Stream = stream,
            MaxInFlight = 1_000_000,
            ConfirmHandler = _ =>
            {
                if (++confirmed % ModPrintMessages == 0)
                {
                    Console.WriteLine($"*****Standard Batch Confirmed: {confirmed}");
                }
            }
        });
        var messages = new List<(ulong, Message)>();
        var start = DateTime.Now;
        for (ulong i = 1; i <= TotalMessages; i++)
        {
            var array = new byte[MessageSize];
            messages.Add((i, new Message(array)));
            if (i % AggregateBatchSize == 0)
            {
                await producer.BatchSend(messages);
                messages.Clear();
            }

            if (i % ModPrintMessages == 0)
            {
                Console.WriteLine($"*****Standard Batch Send: {i}");
            }
        }

        await producer.BatchSend(messages);
        messages.Clear();

        Console.WriteLine(
            $"*****Standard Batch Send***** send time: {DateTime.Now - start}, messages sent: {TotalMessages}");
        Thread.Sleep(1000);
        await producer.Close();
    }

    private static async Task<string> RecreateStream(StreamSystem system, string stream)
    {
        Console.WriteLine("==============================");
        Console.WriteLine($"Recreate Stream: {stream}, just wait a bit..");
        Thread.Sleep(5000);
        try
        {
            await system.DeleteStream(stream);
        }
        catch (Exception)
        {
            // Console.WriteLine(e);
        }

        Thread.Sleep(5000);

        await system.CreateStream(new StreamSpec(stream) { });
        Thread.Sleep(1000);
        Console.WriteLine($"Stream: {stream} created");
        return stream;
    }
}
