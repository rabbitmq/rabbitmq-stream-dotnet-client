using SuperStreamClients;

public static class SwarmSuperStreamExensionMethods
{
    public static IServiceCollection AddSwarmSuperStream(
          this IServiceCollection services,
          IConfiguration namedConfigurationSection,
          Action<RabbitMqStreamOptions> config)
    {
        
        services.Configure<RabbitMqStreamOptions>(namedConfigurationSection);
        services.Configure<RabbitMqStreamOptions>((options)=> {config(options);});

        services.AddSingleton<RabbitMqStreamConnectionFactory>();
        services.AddHostedService<ConsumerBackgroundWorker>();
        services.AddHostedService<ProducerBackgroundWorker>();

        services.AddHttpClient<CustomerAnalyticsClient>((client)=>
        {
            client.BaseAddress = new Uri("http://localhost:5070/CustomerAnalytics");
        });
        return services;
    }
}
