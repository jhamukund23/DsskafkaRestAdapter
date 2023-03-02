using Confluent.Kafka;
using DsskafkaRestAdapter;
using DsskafkaRestAdapter.Application;
using Kafka.Consumer;
using Kafka.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Host.CreateDefaultBuilder(args)
   .ConfigureServices((context, services) =>
   {
       var clientConfig = new ClientConfig()
       {
           BootstrapServers = "pkc-1wvvj.westeurope.azure.confluent.cloud:9092",
           SaslUsername = "MM6SFDFNSFNCKH6J",
           SaslPassword = "RIaHzKEyKxQRwjOdMNW1V4l5hyQ3ZqrfuX+B3Mi+4OMkNbO/xdrAiqbS+pldyNFI",
           SecurityProtocol = SecurityProtocol.SaslSsl,
           SaslMechanism = SaslMechanism.Plain,
           //Acks = Acks.All
       };
   
       var consumerConfig = new ConsumerConfig(clientConfig)
       {
           GroupId = "dss-kafka-Rest-adapter",
           //EnableAutoOffsetStore = false,
           EnableAutoCommit = true,
           AutoOffsetReset = AutoOffsetReset.Earliest,
           StatisticsIntervalMs = 5000,
           SessionTimeoutMs = 6000,
           //EnablePartitionEof = true,
           //PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
       };

       services.AddSingleton(consumerConfig);      
       services.AddSingleton(typeof(IKafkaConsumer<,>), typeof(KafkaConsumer<,>));

       services.AddScoped<IKafkaHandler<string, AddDocument>, MessageHandler>();
       services.AddHostedService<MessageConsumer>();

       services.AddTransient(typeof(IHttpClientWrapper), typeof(HttpClientWrapper));
   })
    .Build()
    .Run();
