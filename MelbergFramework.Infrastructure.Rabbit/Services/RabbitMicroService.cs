using System.Diagnostics;
using MelbergFramework.Application;
using MelbergFramework.Infrastructure.Rabbit.Configuration;
using MelbergFramework.Infrastructure.Rabbit.Consumers;
using MelbergFramework.Infrastructure.Rabbit.Extensions;
using MelbergFramework.Infrastructure.Rabbit.Factories;
using MelbergFramework.Infrastructure.Rabbit.Messages;
using MelbergFramework.Infrastructure.Rabbit.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Events;
namespace MelbergFramework.Infrastructure.Rabbit.Services;
public class RabbitMicroService<TConsumer> : BackgroundService
    where TConsumer : IStandardConsumer
{
    private readonly string _selector;
    private readonly string _metricName;
    private readonly IServiceProvider _serviceProvider;
    private readonly RabbitConfigurationOptions _options;
    private readonly IStandardConnectionFactory _connectionFactory;
    private readonly IMetricPublisher _metricPublisher;
    private readonly ILogger<RabbitMicroService<TConsumer>> _logger;

    public RabbitMicroService(
        string selector,
       IServiceProvider serviceProvider,
        IOptions<RabbitConfigurationOptions> configurationProvider,
        IStandardConnectionFactory connectionFactory,
        IMetricPublisher metricPublisher,
        IOptions<ApplicationConfiguration> applicationOptions,
        ILogger<RabbitMicroService<TConsumer>> logger)
    {
        _selector = selector;
        _metricName = string.Join("_", applicationOptions.Value.Name, _selector, "consumer");
        _serviceProvider = serviceProvider;
        _connectionFactory = connectionFactory;
        _options = configurationProvider.Value;
        _metricPublisher = metricPublisher;
        _logger = logger;
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = RabbitConfigurator.GetConsumerOptions(_selector, _options);
        var channel = await _connectionFactory.GetConsumerModel(_selector);

        await RabbitConfigurator.ConfigureRabbit(channel, _selector, _options);

        var consumer = new AsyncEventingBasicConsumer(channel);
                
        consumer.ReceivedAsync += async (ch, ea) =>
        {
            var message = new Message()
            {
                RoutingKey = ea.RoutingKey,
                Headers = ea.BasicProperties.Headers ?? new Dictionary<string, object>(),
                Body = ea.Body.ToArray()
            };

            await ConsumeMessageAsync(message, stoppingToken);

            await channel.BasicAckAsync(ea.DeliveryTag, false);
        };
        for(int i = 0; i < consumerConfig.Scale; i++)
        {
            var queueName = consumerConfig.Queue;
            await channel.BasicConsumeAsync(consumerConfig.Queue, true, consumerConfig.Name, false, false,new Dictionary<string,object?>(), consumer);
        }
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
    public virtual async Task ConsumeMessageAsync(Message message, CancellationToken cancellationToken)
    {
        var name = _selector + "_consumer";
        Trace.CorrelationManager.StartLogicalOperation(name);
        Trace.CorrelationManager.ActivityId = message.GetCoID();

        var now = DateTime.UtcNow;
        try
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            using (var scope = _serviceProvider.CreateScope())
            {
                await scope
                    .ServiceProvider
                    .GetService<TConsumer>()
                    .ConsumeMessageAsync(message, cancellationToken);
            }
            stopwatch.Stop();
            if (_metricPublisher != null)
            {
                _metricPublisher.SendMetric(_metricName, stopwatch.ElapsedMilliseconds, now);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
        }
        Trace.CorrelationManager.StopLogicalOperation();
    }
}
