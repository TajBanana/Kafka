package Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
  private final Logger logger =
      LoggerFactory.getLogger(Producer.class.getSimpleName());

  public void send() {

    logger.info("Producer.Producer running");

    //  create producer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "cluster.playground.cdkt" +
        ".io:9092");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "");
    properties.setProperty("sasl.mechanism", "PLAIN");

    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
    //  create producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
    //  send data
    for (int i = 0; i < 30 ; i++) {
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
          "demo_java", "same-key" + i);

      kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
        if (e == null) logger.info("received metadata: " + recordMetadata.toString());
        else {logger.error(e.getMessage());}
      });

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    //  flush and close producer
    kafkaProducer.flush();
    kafkaProducer.close();

  }

}
