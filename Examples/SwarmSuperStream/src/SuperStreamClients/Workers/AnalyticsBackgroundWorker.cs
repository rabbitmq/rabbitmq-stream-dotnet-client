namespace SuperStreamClients.Analytics;

public class AnalyticsBackgroundWorker : BackgroundService
{
    private readonly SuperStreamAnalytics _analytics;
    private readonly ILogger<AnalyticsBackgroundWorker> _logger;
    private readonly IOptions<RabbitMqStreamOptions> _options;

    public AnalyticsBackgroundWorker(
        SuperStreamAnalytics analyitics,
        ILogger<AnalyticsBackgroundWorker> logger,
         IOptions<RabbitMqStreamOptions> options
    )
    {
        _analytics = analyitics;
        _logger = logger;
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_options.Value.Analytics)
        {
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogInformation(_analytics.ToString());

                    try
                    {
                        await Task.Delay(1000, stoppingToken);
                    }
                    catch (TaskCanceledException ex)
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failure. Analytics worker terminating.");
            }
        }
    }
}
