namespace SuperStreamClients;

public class RabbitMqStreamConnectionFactory
{
    ILogger<RabbitMqStreamConnectionFactory> _logger;
    IOptions<RabbitMqStreamOptions> _options;

    public RabbitMqStreamConnectionFactory(
        ILogger<RabbitMqStreamConnectionFactory> logger,
        IOptions<RabbitMqStreamOptions> options)
    {
        _logger = logger;
        _options = options;
    }

    public async Task<StreamSystem> Create(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var config = await GetConnectionConfig();
                var streamSystem = await StreamSystem.Create(config);
                return streamSystem;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to connect to host. Will retry.");
                try
                {
                    await Task.Delay(1000, cancellationToken);
                }
                catch (TaskCanceledException tce)
                {
                    break;
                }
            }
        }

        throw new Exception("Failed to connect.");
    }

    public async Task<StreamSystemConfig> GetConnectionConfig()
    {
        var options = _options.Value;
        var hostEntry = await Dns.GetHostEntryAsync(options.HostName);
        var config = new StreamSystemConfig
        {
            UserName = options.UserName,
            Password = options.Password,
            VirtualHost = options.VirtualHost,
            Endpoints = GetEndpoints(hostEntry, options.StreamPort).ToList()
        };
        return config;
    }

    private IEnumerable<EndPoint> GetEndpoints(IPHostEntry hostEntry, int streamPort)
    {
        var endpoints = hostEntry.AddressList.Select(address => new IPEndPoint(address, streamPort));
        foreach (var endpoint in endpoints)
        {
            _logger.LogInformation("Using endpoint {endpoint}:{port}", endpoint.Address, endpoint.Port);
            yield return (EndPoint)endpoint;
        }
    }
}