using MelbergFramework.Core.ComponentTesting;
using MelbergFramework.Core.DependencyInjection;
using MelbergFramework.Infrastructure.Rabbit.Configuration;
using MelbergFramework.Infrastructure.Rabbit.Messages;
using MelbergFramework.Infrastructure.Rabbit.Metrics;
using MelbergFramework.Infrastructure.Rabbit.Publishers;
using MelbergFramework.Infrastructure.Rabbit.Translator;
using Microsoft.Extensions.DependencyInjection;

namespace MelbergFramework.ComponentTesting.Rabbit;

public static class DependencyModule
{
    public static IServiceCollection PrepareConsumer<IStandardConsumer>
        (this IServiceCollection services)

    {
        CoreComponentDependencyModule.PrepareApplication(services);
        services.Configure<RabbitConfigurationOptions>(_ =>
        {
            _.ClientDeclarations = new()
            {
                Connections = new ()
                {
                    
                }

            };
            _.ServerDeclarations = new();
        });


        return services
            .OverridePublisher<MetricMessage>();
    }
    public static IServiceCollection OverridePublisher<TMessage>
        (this IServiceCollection services)
    where TMessage : IStandardMessage
    {
        return services
            .OverrideWithSingleton
            <IStandardPublisher<TMessage>, MockPublisher<TMessage>>();
    }

    public static IServiceCollection OverrideTranslator<TMessage>
        (this IServiceCollection services)
    where TMessage : IStandardMessage
    {
        return services
            .OverrideWithSingleton
            <IJsonToObjectTranslator<TMessage>, MockTranslator<TMessage>>();
    }
}
