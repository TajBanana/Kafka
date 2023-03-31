import Consumer.Consumer;
import Producer.ProducerSameKey;

public class Main {
  public static void main(String[] args) {
/*    Producer.Producer producer = new Producer.Producer();
    producer.send();*/

    Consumer consumer = new Consumer();
    consumer.get();

  }
}
