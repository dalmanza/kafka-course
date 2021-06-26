package com.dan.dev.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DevConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DevConsumer.class);
  
  private static final String TOPIC_NAME = "dan-kafka-01";

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
    try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties())){
      consumer.subscribe(Arrays.asList(TOPIC_NAME));
      while (true) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
        
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
          LOGGER.info(""
              + "OFFSET = {},"
              + "PARTITION = {},"
              + "KEY = {},"
              + "VALUE = {}",
              consumerRecord.offset(),
              consumerRecord.partition(),
              consumerRecord.key(),
              consumerRecord.value());
        }
        
      }
    }
    
  }

}
