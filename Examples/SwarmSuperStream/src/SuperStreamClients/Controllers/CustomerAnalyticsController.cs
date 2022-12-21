using Microsoft.AspNetCore.Mvc;

namespace SuperStreamClients.Controllers;

[ApiController]
[Route("[controller]")]
public class CustomerAnalyticsController : ControllerBase
{

    private readonly ILogger<CustomerAnalyticsController> _logger;

    public CustomerAnalyticsController(ILogger<CustomerAnalyticsController> logger)
    {
        _logger = logger;
    }

    [HttpPost(Name = "PostCustomerMessage")]
    public async Task Post(ReceivedCustomerMessage message, CancellationToken ct = default(CancellationToken))
    {
        _logger.LogInformation("Analytics got message {message}", message.ToString());
    }
}
