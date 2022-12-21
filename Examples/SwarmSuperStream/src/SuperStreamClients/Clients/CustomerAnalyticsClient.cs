namespace SuperStreamClients;

public class CustomerAnalyticsClient
{
    private readonly HttpClient _client;
    public CustomerAnalyticsClient (HttpClient client)
    {
        _client = client;
    }

    public async Task Post(ReceivedCustomerMessage message)
    {
        await _client.PostAsJsonAsync(string.Empty, message);
    }
}
