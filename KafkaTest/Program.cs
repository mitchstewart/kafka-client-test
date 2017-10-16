using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Kafka.Client.Cfg;
using Kafka.Client.Producers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaTest
{
    class Program
    {
        static void Main(string[] args)
        {
            MicrosoftKafkaTest();
            //ConfluenceKafkaTest();
        }

        private static void MicrosoftKafkaTest()
        {
            var brokerConfig = new BrokerConfiguration()
            {
                Host = "0.tcp.ngrok.io",
                Port = 15069
            };
            var config = new ProducerConfiguration(new List<BrokerConfiguration> { brokerConfig });
            var producer = new Kafka.Client.Producers.Producer(config);

            var payload = Encoding.ASCII.GetBytes(@"
            { 
                'method': 'POST', 
                'timeStamp': 1508164715, 
                'foo': 
                    { 
                        'id': 123 
                    } 
            }");
            var message = new Kafka.Client.Messages.Message(payload);
            var data = new ProducerData<string, Kafka.Client.Messages.Message>("fun-topic", message);

            try
            {
                producer.Send(data);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static void ConfluenceKafkaTest()
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", "0.tcp.ngrok.io:15069" },
            };

            var producer = new Confluent.Kafka.Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));

            try
            {
                var deliveryReport = producer.ProduceAsync("fun-topic", null, "Hello from Windows").Result;
                Console.WriteLine($"Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                producer.Dispose();
            }
        }
    }
}
