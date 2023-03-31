package Consumer;

import Producer.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
  private final Logger logger = LoggerFactory.getLogger(Consumer.class.getSimpleName());
  String groupId = "my-java-consumer";
  String topic = "demo_java";

  public void get() {

    logger.info("Consumer running");

    //  create consumer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "");
    properties.setProperty("sasl.mechanism", "PLAIN");

    //  create consumer config
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());

    properties.setProperty("group.id", groupId);
    properties.setProperty("auto.offset.reset", "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(topic));

    while (true) {
      logger.info("polling");
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

      for (ConsumerRecord<String, String> record : records) {
        logger.info("key: " + record.key());
        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
      }

    }
  }


}
