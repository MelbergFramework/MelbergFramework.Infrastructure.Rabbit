using MelbergFramework.Core.HealthCheck;
using MelbergFramework.Infrastructure.Rabbit.Factories;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace MelbergFramework.Infrastructure.Rabbit.Health;

public class RabbitConsumerHealthCheck : HealthCheck
{
    private readonly string _name;
    private readonly IChannel _connection;
    public RabbitConsumerHealthCheck(IServiceProvider serviceProvider, string name = "IncomingMessages")
    {
        _name = name;
        var intermediate = serviceProvider.GetService<IStandardConnectionFactory>().GetConsumerModel(name);
        intermediate.Wait();
        _connection = intermediate.Result;
    }

    public override string Name => "rabbitconsumer_"+_name;
    public override Task<bool> IsOk(CancellationToken token)
    {
        return Task.FromResult(_connection.IsOpen);    
    }
}
