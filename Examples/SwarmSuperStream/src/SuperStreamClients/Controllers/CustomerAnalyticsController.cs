using Microsoft.AspNetCore.Mvc;
using SuperStreamClients.Analytics;

namespace SuperStreamClients.Controllers;

[ApiController]
[Route("[controller]")]
public class CustomerAnalyticsController : ControllerBase
{
    private readonly ILogger<CustomerAnalyticsController> _logger;
    private readonly SuperStreamAnalytics _analytics;
    public CustomerAnalyticsController(
        ILogger<CustomerAnalyticsController> logger,
        SuperStreamAnalytics analytics)
    {
        _logger = logger;
        _analytics = analytics;
    }

    [HttpPost(Name = "PostCustomerMessage")]
    public  ReceivedCustomerMessageResponse Post(ReceivedCustomerMessage message, CancellationToken ct = default(CancellationToken))
    {
        _analytics.NewMessage(message);
        _logger.LogInformation("Analytics got message {message}", message.ToString());
        var response = new ReceivedCustomerMessageResponse()
        {
            Host = Environment.MachineName,
            Message = $"Thanks for message {message.CustomerMessage.MessageNumber}"
        };

        return response;
    }
}
