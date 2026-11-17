using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStreamSingleActiveConsumer;

public static class SuperStreamProducer
{
    public static async Task Start(StreamSystem system, ILogger logger, int messageCount)
    {
        var confirmed = 0;
        var done = new TaskCompletionSource();

        var producer = await Producer.Create(new ProducerConfig(system, Program.SuperStreamName)
        {
            SuperStreamConfig = new SuperStreamConfig { Routing = message => message.Properties.MessageId.ToString() },
            ConfirmationHandler = confirmation =>
            {
                if (confirmation.Status != ConfirmationStatus.Confirmed)
                {
                    logger.LogError("Message not confirmed: {Status}", confirmation.Status);
                }

                var count = Interlocked.Increment(ref confirmed);
                if (count % 100_000 == 0)
                {
                    logger.LogInformation("Confirmed {Count}/{Total} messages", count, messageCount);
                }

                if (count >= messageCount)
                {
                    done.TrySetResult();
                }

                return Task.CompletedTask;
            }
        }).ConfigureAwait(false);

        logger.LogInformation("Sending {Total} messages to {SuperStream}", messageCount, Program.SuperStreamName);
        for (var i = 0; i < messageCount; i++)
        {
            await producer.Send(new Message(Encoding.UTF8.GetBytes($"message {i}"))
            {
                Properties = new Properties { MessageId = $"key-{i}" }
            }).ConfigureAwait(false);
        }
        await Task.Delay(100).ConfigureAwait(false);
        await done.Task.ConfigureAwait(false);
        logger.LogInformation("All {Total} messages confirmed", messageCount);
        await producer.Close().ConfigureAwait(false);
    }
}
