using System;
using System.Buffers;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace example;

public class ReliableProducerExample
{
    public async Task StartDefaultConfigurations()
    {
        var config = new StreamSystemConfig();
        const string stream = "my-reliable-stream";
        var system = await StreamSystem.Create(config);
        await system.CreateStream(new StreamSpec(stream)
        {
            MaxLengthBytes = 2073741824
        });
        const int totalMessages = 1_000;
       
        var reliableProducer = await ReliableProducer.CreateReliableProducer(new ReliableProducerConfig()
        {
            StreamSystem = system,
            Stream = stream,
            Reference = "my-reliable-producer",
            ConfirmationHandler = confirmation =>
            {
                Console.WriteLine(confirmation.Status == ConfirmationStatus.Confirmed
                    ? $"Confirmed: Publishing id {confirmation.PublishingId}"
                    : $"Not Confirmed: Publishing id {confirmation.PublishingId}, error: {confirmation.Status} ");
                return Task.CompletedTask;
            }
        });
        var start = DateTime.Now;
        for (var i = 0; i < totalMessages; i++)
        {
            // standard send
            await reliableProducer.Send(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        // sub-batch
        var listMessages = new List<Message>();
        for (var i = 0; i < 100; i++)
        {
            listMessages.Add(new Message(Encoding.UTF8.GetBytes($"hello {i}")));
        }

        // in this case you will receive back one confirmation with all the messages
        await reliableProducer.Send(listMessages, CompressionType.Gzip);


        Console.WriteLine($"End...Done {DateTime.Now - start}");
        // just to receive all the notification back
        Thread.Sleep(TimeSpan.FromSeconds(2));
        await reliableProducer.Close();
        
    }
}
