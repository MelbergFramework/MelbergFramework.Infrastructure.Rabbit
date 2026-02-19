using MelbergFramework.Infrastructure.Rabbit.Common.Exceptions;
using MelbergFramework.Infrastructure.Rabbit.Configuration;
using RabbitMQ.Client;

namespace MelbergFramework.Infrastructure.Rabbit.Extensions;

public static class RabbitConfigurator
{
    public static RecieverOptions GetConsumerOptions(string selector, RabbitConfigurationOptions configurationOptions)
    {
        var receiverConfigs = configurationOptions.ClientDeclarations.AsyncRecievers.Where(_ => _.Name == selector );
        
        if(!receiverConfigs.Any())
            throw new ConsumerConfigurationNotFoundException($"Consumer configuration for {selector} not found.");

        return receiverConfigs.First();
    }
    
    public static PublisherOptions GetPublisherOptions(string selector, RabbitConfigurationOptions configurationOptions)
    {
        var publisherConfigs = configurationOptions.ClientDeclarations.Publishers.Where(_ => _.Name == selector);
        
        if(!publisherConfigs.Any())
            throw new PublisherConfigurationNotFoundException($"Publisher configuration for {selector} not found.");
        
        return publisherConfigs.First();
    }
    
    public static async Task ConfigureRabbit(this IChannel channel, string selector, RabbitConfigurationOptions configurationOptions)
    {
        var receiverConfigs = GetConsumerOptions(selector, configurationOptions);
        
        await ConfigureExchanges(channel,receiverConfigs.Connection,configurationOptions.ServerDeclarations.Exchanges);
        await ConfigureQueues(channel, receiverConfigs.Connection,configurationOptions.ServerDeclarations.Queues);
        await ConfigureBindings(channel, receiverConfigs.Connection, configurationOptions.ServerDeclarations.Bindings);
    }
    static async Task ConfigureExchanges(this IChannel Channel, string Connection, IEnumerable<ExchangeOptions> ExchangeInfo)
    {
        var relevantExchanges = ExchangeInfo.Where(_ => _.Connection == Connection).ToList();

        foreach(var exchange in relevantExchanges)
        {
            await Channel.ExchangeDeclareAsync(exchange.Name,ToExchangeType(exchange.Type),exchange.Durable,exchange.AutoDelete);
        }
    }

    static async Task ConfigureQueues(this IChannel Channel, string Connection, IEnumerable<QueueOptions> QueueData)
    {
        var relevantQueues = QueueData.Where(_ => _.Connection == Connection).ToList();

        foreach(var queue in relevantQueues)
        {
            await Channel.QueueDeclareAsync(queue.Name,queue.Durable,queue.Exclusive,queue.AutoDelete);
        }
    }

    static async Task ConfigureBindings(this IChannel Channel, string Connection, IEnumerable<BindingOptions> BindingData)
    {
        var relevantBindings = BindingData.Where(_ => _.Connection == Connection).ToList();

        foreach(var binding in relevantBindings)
        {
            await Channel.QueueBindAsync(binding.Queue,binding.Exchange,binding.SubscriptionKey);
        }
    }
    static string ToExchangeType(this string configType)
    {
        switch(configType.ToLower())
        {
            case "direct": return ExchangeType.Direct;
            case "fanout": return ExchangeType.Fanout;
            case "topic":  return ExchangeType.Topic;
        }
        throw new Exception("An invalid exchange type has been given");
    }
}
