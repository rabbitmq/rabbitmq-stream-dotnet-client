namespace SuperStreamClients;

public class CustomerAnalyticsClient
{
    private readonly HttpClient _client;
    private readonly ILogger<CustomerAnalyticsClient> _logger;
    public CustomerAnalyticsClient(
        HttpClient client,
        ILogger<CustomerAnalyticsClient> logger)
    {
        _client = client;
        _logger = logger;
    }

    public async Task Post(
        ReceivedCustomerMessage message,
        CancellationToken cancellationToken = (default))
    {
        var response = await _client.PostAsJsonAsync(string.Empty, message, cancellationToken);
        var responseContent = await response.Content.ReadFromJsonAsync<ReceivedCustomerMessageResponse>();
        _logger.LogInformation("Sent data to {client} with status {status} and response {responseContent}",
            response.RequestMessage.RequestUri,
            response.StatusCode,
            responseContent);
    }
}
