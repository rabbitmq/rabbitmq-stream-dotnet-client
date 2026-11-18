// Example: super stream + single active consumer + ConsumerUpdateListener + SubscriptionListener.
//
// The offsets are saved in a simulated external storage (OffsetStore, an in-memory map) and both listeners
// log the offset the consumer is going to restart from.
//
// Usage:
//   dotnet run -- --producer [messageCount]   publish messageCount messages (default 2000000) and exit
//   dotnet run -- --consumer                  start a single active consumer on every partition
//
// Run several --consumer instances: only one is active for each partition. Stop the active one
// and watch the ConsumerUpdateListener / SubscriptionListener logs in the others.
// (Each process has its own in-memory store, so a new active instance restarts from "first".
// A real external storage shared between the instances would let it continue where the other stopped.)

using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using SuperStreamSingleActiveConsumer;

internal class Program
{
    public const string SuperStreamName = "sac-super-stream-example";
    private const int DefaultMessageCount = 2_000_000;

    private static async Task Main(string[] args)
    {
        var mode = args.Length > 0 ? args[0] : string.Empty;
        if (mode is not ("--producer" or "--consumer"))
        {
            Console.WriteLine("Unknown option, valid options: --producer [messageCount] / --consumer");
            return;
        }

        var messageCount = DefaultMessageCount;
        if (args.Length > 1 && (!int.TryParse(args[1], out messageCount) || messageCount <= 0))
        {
            Console.WriteLine("messageCount must be a positive integer");
            return;
        }

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(o => o.TimestampFormat = "HH:mm:ss.fff ");
            builder.AddFilter("RabbitMQ.Stream", LogLevel.Information);
        });
        var logger = loggerFactory.CreateLogger("SacSuperStream");

        var system = await StreamSystem.Create(new StreamSystemConfig(), loggerFactory.CreateLogger<StreamSystem>())
            .ConfigureAwait(false);
        try
        {
            // Safe to call from every instance: an already existing super stream is not an error for the example.
            await system.CreateSuperStream(new PartitionsSuperStreamSpec(SuperStreamName, 3)).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            logger.LogInformation("Super stream not created (probably already exists): {Message}", e.Message);
        }

        if (mode == "--producer")
        {
            await SuperStreamProducer.Start(system, logger, messageCount).ConfigureAwait(false);
        }
        else
        {
            await SuperStreamSacConsumer.Start(system, logger,
                loggerFactory.CreateLogger<RabbitMQ.Stream.Client.Reliable.Consumer>()).ConfigureAwait(false);
        }

        await system.Close().ConfigureAwait(false);
    }
}
