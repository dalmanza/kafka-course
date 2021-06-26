package com.dan.dev.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DevProducer {

  private static final String TOPIC_NAME = "dan-kafka-01";

  public static Properties loadProperties () {
    Properties props = new Properties();
    
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    props.put(ProducerConfig.ACKS_CONFIG, "1"); //un acks de al menos un broker - usar all cuando quiero que todas las replicas confirmen que el mensaje esta en las particiones 
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }
  
  public static void main(final String[] args) {
    
    //create a producer using a try with resources to guarantee that the method .close() is called.
    try(Producer<String, String> producer = new KafkaProducer<>(loadProperties())){
      for (int i = 0; i < 1000000; i++) {
        //Llamado Async! better perfomance
        producer.send(new ProducerRecord<String, String>(TOPIC_NAME, String.valueOf(i), "value-01"));
        //Llamado sync! takes a lot more time to send all messages to kafka. Also, needs to add an extra cath.
        //producer.send(new ProducerRecord<String, String>(TOPIC_NAME, String.valueOf(i), "value-01")).get();
      }
      producer.flush(); //Send all messages in one shot. 
    }
    
  }
}
