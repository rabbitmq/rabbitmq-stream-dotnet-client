using System.Text;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace SuperStreamClients;

public class ProducerBackgroundWorker : BackgroundService
{
    private readonly ILogger<ProducerBackgroundWorker> _logger;
    private readonly RabbitMqStreamConnectionFactory _systemConnection;
    private readonly IOptions<RabbitMqStreamOptions> _options;
    private readonly Random _random;
    StreamSystem _streamSystem;
    Producer _producer;

    public ProducerBackgroundWorker(
        ILogger<ProducerBackgroundWorker> logger,
        RabbitMqStreamConnectionFactory systemConnection,
        IOptions<RabbitMqStreamOptions> options
    )
    {
        _logger = logger;
        _systemConnection = systemConnection;
        _options = options;
        var optionsValue = _options.Value;
        _random = new Random(optionsValue.RandomSeed);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await CreateConnection();
        await CreateProducer(stoppingToken);
        await SendMessages(stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _producer?.Close();
        await _streamSystem?.Close();
        await base.StopAsync(cancellationToken);
    }

    private async Task SendMessages(CancellationToken ct)
    {
        var options = _options.Value;
        for (int messageId = 0; messageId < options.ProducerMessageSendLimit; messageId++)
        {
            if (ct.IsCancellationRequested)
                break;

            await SendMessage(messageId);
            await DelayNextSend(options, ct);
        };
    }

    private async Task SendMessage(int messageNumber)
    {
        var options = _options.Value;
        var message = CreateMessage(messageNumber, options);
        await _producer.Send(message);
        _logger.LogInformation("Written Message {messageNumber}", messageNumber);

    }

    private async Task DelayNextSend(RabbitMqStreamOptions options, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(
                _random.Next(options.ProducerSendDelayMin, options.ProducerSendDelayMax),
                cancellationToken);
        }
        catch (TaskCanceledException ex)
        {
            _logger.LogError("Delay canceled due cancellation requested");
        }
    }

    private Message CreateMessage(int messageNumber, RabbitMqStreamOptions options)
    {
        var customerId = _random.Next(0, options.NumberOfCustomers);
        var orderId = Guid.NewGuid();
        var message = new Message(SerializeProducedMessage(messageNumber, customerId, orderId))
        {
            Properties = new Properties() { MessageId = CreateMessageId(customerId) }
        };
        return message;
    }

    private string CreateMessage(int messageNumber, int customerId, Guid orderId) =>
        $"Some message about the order here";

    private byte[] SerializeProducedMessage(int messageNumber, int customerId, Guid orderId)
    {
        var message = CreateProducedMessage(messageNumber, customerId, orderId);
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message);
    }

    private CustomerMessage CreateProducedMessage(int messageNumber, int customerId, Guid orderId)
    {
        var hostName = GetHostName();
        var data = CreateMessage(messageNumber, customerId, orderId);

        var message = new CustomerMessage(
            hostName,
            customerId,
            orderId,
            messageNumber,
            data);

        return message;
    }

    private byte[] SerializeProducedMessage(CustomerMessage message)
    {
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message);
    }

    private string GetHostName() => System.Environment.MachineName;


    private string CreateMessageId(int customerId) =>
        customerId.ToString();


    private async Task CreateConnection()
    {
        var config = await _systemConnection.Create();
        _streamSystem = await StreamSystem.Create(config);
    }

    private async Task CreateProducer(CancellationToken cancellationToken)
    {
        var options = _options.Value;

        _producer = await Producer.Create(new ProducerConfig(_streamSystem, options.StreamName)
        {
            SuperStreamConfig = new SuperStreamConfig()
            {
                Routing = message1 => message1.Properties.MessageId.ToString()
            }
        });
    }




}
