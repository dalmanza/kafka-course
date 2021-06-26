package com.dan.dev.kafka.transactional;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalProducer.class);

  private static final String TOPIC_NAME = "dan-kafka-01";

  public static Properties loadProperties() {
    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    props.put(ProducerConfig.ACKS_CONFIG, "all"); // un acks de al menos un broker - usar all cuando
                                                  // quiero que todas las replicas confirmen que el
                                                  // mensaje esta en las particiones
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "dan-producer-id");
    return props;
  }

  public static void main(final String[] args) {

    // create a producer using a try with resources to guarantee that the method .close() is called.
    try (Producer<String, String> producer = new KafkaProducer<>(loadProperties())) {
      try {
        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < 100; i++) {
          // Llamado Async! better perfomance:
          producer
              .send(new ProducerRecord<String, String>(TOPIC_NAME, "value-01"));
          if (i == 50) {
            throw new Exception("The message received had an error!");
          }
        }
        producer.commitTransaction(); // En este caso cuando lo 100K msgs sean enviados a kafka, se
                                      // hace hace commit de la transaccion.
        // Nota: En este caso el commit no se realiza por mesanje, Se realiza por el conjunto de
        // transacciones.
//        producer.flush(); // Send all messages in one shot.
      } catch (Exception e) {
        LOGGER.error("There was an error in the transaction ", e);
        producer.abortTransaction();
      }
    }
  }
}

/**
 * To make a producer transactional the following methods need to be added: 1.
 * producer.initTransactions(); indicates the transactions has been initiated but hasnt started. 2.
 * producer.beginTransaction(); Once the transaction has initiated can begin, between these 2
 * methods can be added logic. 3. producer.commitTransaction(); use this method if everything went
 * well. 4. producer.abortTransaction();; use this method when an error occurs
 */
