using System.Diagnostics;
using MelbergFramework.Infrastructure.Rabbit.Configuration;
using MelbergFramework.Infrastructure.Rabbit.Extensions;
using MelbergFramework.Infrastructure.Rabbit.Factories;
using MelbergFramework.Infrastructure.Rabbit.Messages;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace MelbergFramework.Infrastructure.Rabbit.Publishers;

public abstract class BasePublisher<TMessage>
    where TMessage :  IStandardMessage
{
    private IChannel _channel;
    protected IChannel Channel 
    {
        get
        {
            if(_channel == null)
            {
                var result = _connectionFactory.GetPublisherChannel(typeof(TMessage).Name).CreateChannelAsync();
                result.Wait();
                _channel = result.Result;
            }
            return _channel;
        }
    }

    private readonly IStandardConnectionFactory _connectionFactory;
    private readonly PublisherOptions _config;
    private bool _disposed;


    public BasePublisher(IOptions<RabbitConfigurationOptions> configuration)
    {
        _config = RabbitConfigurator.GetPublisherOptions(typeof(TMessage).Name, configuration.Value);
        _connectionFactory = new StandardConnectionFactory(configuration);
    }


    public async ValueTask Emit(Message message)
    {
        var properties = new BasicProperties();
        
        properties.Headers = message.Headers;
        
        properties.Headers.TryAdd(MessageExtensions.Headers.Timestamp,
                DateTime.UtcNow.ToString());

        properties.Headers[MessageExtensions.Headers.CorrelationId] = 
            _config.MaintainCorrelation ?
                Trace.CorrelationManager.ActivityId.ToString() : 
                Guid.NewGuid().ToString();

        await Channel.BasicPublishAsync(
            _config.Exchange,
            message.RoutingKey,
            true,
            properties,
            message.Body);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        if (disposing && _channel != null)
        {
            _channel.CloseAsync().Wait();
        }

        _disposed = true;
    }
}
