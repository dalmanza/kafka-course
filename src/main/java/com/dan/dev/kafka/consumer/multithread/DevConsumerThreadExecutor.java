package com.dan.dev.kafka.consumer.multithread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class DevConsumerThreadExecutor {

  
  private static final int NUMBER_THREADS = 5;
  
  public static Properties getProperties () {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dan-kafka-group");//Consumer group can have multiple clients
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }



  public static void main(final String[] args) {
    ExecutorService executor = Executors.newFixedThreadPool(NUMBER_THREADS); //Created 5 threads because the topic has 5 partitions.
    
    for (int i = 0; i < NUMBER_THREADS; i++) {
      DevConsumerThread consumer = new DevConsumerThread(new KafkaConsumer<>(getProperties()));
      executor.execute(consumer);
    }
    
    while (!executor.isTerminated());

  }

}
