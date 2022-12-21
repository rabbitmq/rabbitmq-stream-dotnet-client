using System.Buffers;
using System.Text;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStreamClients.Consumers;
public class ConsumerBackgroundWorker : BackgroundService
{
    private readonly ILogger<ConsumerBackgroundWorker> _logger;
    private readonly RabbitMqStreamConnectionFactory _systemConnection;
    private readonly IOptions<RabbitMqStreamOptions> _options;
    private readonly Random _random;
    private readonly CustomerAnalyticsClient _analyticsClient;
    StreamSystem _streamSystem;
    Consumer _consumer;

    public ConsumerBackgroundWorker(
        ILogger<ConsumerBackgroundWorker> logger,
        RabbitMqStreamConnectionFactory systemConnection,
        IOptions<RabbitMqStreamOptions> options,
        CustomerAnalyticsClient analyticsClient
    )
    {
        _logger = logger;
        _systemConnection = systemConnection;
        _options = options;
        _analyticsClient = analyticsClient;
        var optionsValue = _options.Value;
        _random = new Random(optionsValue.RandomSeed);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_options.Value.Consumer)
        {
            try
            {
                await CreateConnection(stoppingToken);
                await CreateConsumer(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failure. Consumer worker terminating.");
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_options.Value.Consumer)
        {
            await _consumer?.Close();
            await _streamSystem?.Close();
        }
        await base.StopAsync(cancellationToken);
    }

    private async Task CreateConnection(CancellationToken cancellationToken)
    {
        _streamSystem = await _systemConnection.Create(cancellationToken);
    }

    public async Task<Consumer> CreateConsumer(CancellationToken cancellationToken)
    {
        var options = _options.Value;
        _consumer = await Consumer.Create(
            new ConsumerConfig(_streamSystem, options.StreamName)
            {
                IsSuperStream = true,
                IsSingleActiveConsumer = true,
                Reference = options.ConsumerAppReference,
                OffsetSpec = new OffsetTypeNext(),

                MessageHandler = async (sourceStream, consumer, ctx, message) =>
                {
                    await MessageHandle(sourceStream, consumer, ctx, message, options, cancellationToken);
                }
            });
        _logger.LogInformation("Consumer created");
        return _consumer;
    }

    public virtual async Task MessageHandle(
        string sourceStream,
        RawConsumer consumer,
        MessageContext ctx,
        Message message,
        RabbitMqStreamOptions options,
        CancellationToken cancellationToken)
    {
        var hostName = GetHostName();
        var customerId = GetCustomerId(message);
        var receivedMessage = CreateReceivedCustomerMessage(sourceStream, message);

        var postAnalticsTask = PostAnalytics(receivedMessage);
        var delayTask = DelayMessageHandling(options, cancellationToken);
        LogMessageReceived(hostName, sourceStream, customerId, receivedMessage);
        await Task.WhenAll(postAnalticsTask, delayTask);

    }

    private string GetHostName() => System.Environment.MachineName;

    private int GetCustomerId(Message message)
    {
        var customerIdString = message.Properties.MessageId.ToString();
        var customerId = int.Parse(customerIdString);
        return customerId;
    }

    private void LogMessageReceived(
        string hostName,
        string sourceStream,
        int customerId,
        ReceivedCustomerMessage data)
    {
        _logger.LogInformation(
            "Host: {hostName} SourceStream: {sourceStream}:\t To: {customerId}\t Message: {data}",
            hostName,
            sourceStream,
            customerId,
            data
            );
    }

    private ReceivedCustomerMessage CreateReceivedCustomerMessage(
        string sourceStream,
        Message rabbitmqMessage)
    {
        var customerMessage = DecodeProducedData(rabbitmqMessage);
        return CreateReceivedCustomerMessage(sourceStream, customerMessage);
    }

    private ReceivedCustomerMessage CreateReceivedCustomerMessage(
        string sourceStream,
        CustomerMessage sentMessage)
    {
        var hostName = GetHostName();
        return new ReceivedCustomerMessage(
            TimeOnly.FromDateTime(DateTime.Now),
            hostName, 
            sourceStream, 
            sentMessage);
    }

    private CustomerMessage DecodeProducedData(Message message)
    {
        var data = message.Data.Contents.ToArray();
        return System.Text.Json.JsonSerializer.Deserialize<CustomerMessage>(data);
    }

    private async Task PostAnalytics(
        ReceivedCustomerMessage message
    )
    {
        await _analyticsClient.Post(message);
    }

    private async Task DelayMessageHandling(
        RabbitMqStreamOptions options,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(
                _random.Next(options.ConsumerHandleDelayMin, options.ConsumerHandleDelayMax),
                cancellationToken);
        }
        catch (TaskCanceledException ex)
        {
            _logger.LogError("Handle delay canceled due cancellation requested");
        }
    }
}