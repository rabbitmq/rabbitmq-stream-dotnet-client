using System.Diagnostics;

namespace SuperStreamClients.Analytics;

public class AnalyticsBackgroundWorker : BackgroundService
{
    private readonly SuperStreamAnalytics _analytics;
    private readonly ILogger<AnalyticsBackgroundWorker> _logger;
    private readonly IOptions<RabbitMqStreamOptions> _options;

    public AnalyticsBackgroundWorker(
        SuperStreamAnalytics analytics,
        ILogger<AnalyticsBackgroundWorker> logger,
         IOptions<RabbitMqStreamOptions> options
    )
    {
        _analytics = analytics;
        _logger = logger;
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (IsAnalyticsEnabled())
        {
            await TryRunAnalyticsLoop(stoppingToken);
        }
    }

    private bool IsAnalyticsEnabled() => _options.Value.Analytics;

    private async Task TryRunAnalyticsLoop(CancellationToken cancellationToken)
    {
        try
        {
            await RunAnalyticsLoop(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failure. Analytics worker terminating.");
        }
    }

    private async Task RunAnalyticsLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            RunAnalytics(cancellationToken);
            await DelayForAnalyticsRefreshTime(cancellationToken);
        }
    }

    private async Task DelayForAnalyticsRefreshTime(CancellationToken cancellationToken)
    {
        try
        {
            var delayTime = _options.Value.AnalyticsRefreshTime;
            _logger.LogDebug("Analytics refresh delaying for {analyticsDelayTime} ms", delayTime.TotalMilliseconds);
            await Task.Delay(delayTime, cancellationToken);
        }
        catch (TaskCanceledException ex)
        {
            _logger.LogInformation("Analytics requested to stop");
        }
    }

    private void RunAnalytics(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Analytics run starting");
        var sw = Stopwatch.StartNew();
        _logger.LogInformation(_analytics.GenerateSummaryInfo(cancellationToken));
        sw.Stop();
        _logger.LogDebug("Analytics ran in {analyticsRunTime} ms", sw.ElapsedMilliseconds);
    }
}