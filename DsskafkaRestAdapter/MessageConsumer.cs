using Confluent.Kafka;
using DsskafkaRestAdapter.Application.Constants;
using Kafka.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DsskafkaRestAdapter
{
    public class MessageConsumer : BackgroundService
    {
        private readonly IKafkaConsumer<string, AddDocument> _consumer;
        public MessageConsumer(IKafkaConsumer<string, AddDocument> kafkaConsumer)
        {
            _consumer = kafkaConsumer;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await _consumer.Consume(KafkaTopics.AddDocumentRequest, stoppingToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{(int)HttpStatusCode.InternalServerError} ConsumeFailedOnTopic - {KafkaTopics.AddDocumentRequest}, {ex}");
            }
        }

        public override void Dispose()
        {
            _consumer.Close();
            _consumer.Dispose();

            base.Dispose();
        }
    }
}
