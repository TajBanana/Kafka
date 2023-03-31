package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutDown {
  private final Logger logger = LoggerFactory.getLogger(ConsumerWithShutDown.class.getSimpleName());
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

    final Thread thread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        logger.info("Detected a shutdown, calling consumer.wakeup()...");
        consumer.wakeup();

      //  join the main thread to allow execution of the code in main thread
        try {
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });



    consumer.subscribe(Arrays.asList(topic));

    try {
      while (true) {
        logger.info("polling");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          logger.info("key: " + record.key());
          logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
        }

      }

    } catch (WakeupException e) {
      logger.info("consumer is shutting down");
    } catch (Exception e) {
      logger.error("Unexpected exception");
    } finally {
      consumer.close();
      logger.info("consumer is closed");
    }

  }


}
