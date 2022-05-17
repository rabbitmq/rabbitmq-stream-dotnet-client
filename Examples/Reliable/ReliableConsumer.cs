using System.Buffers;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace example;

public class ReliableConsumerExample
{
    public async Task StartDefaultConfigurations()
    {
        Console.WriteLine("Reliable .NET Cosumer");
        var config = new StreamSystemConfig();
        const string stream = "my-reliable-stream-consumer";
        var system = await StreamSystem.Create(config);
        await system.CreateStream(new StreamSpec(stream));
        var rConsumer = await ReliableConsumer.CreateReliableConsumer(new ReliableConsumerConfig()
        {
            Stream = stream,
            StreamSystem = system,
            OffsetSpec = new OffsetTypeFirst(),
            ClientProvidedName = "My-Reliable-Consumer",
            MessageHandler = async (_, _, message) =>
            {
                Console.WriteLine(
                    $"message: {Encoding.Default.GetString(message.Data.Contents.ToArray())}");
                await Task.CompletedTask;
            }
        });
    }
    
    // when finished:
    //rConsumer.Close();
}