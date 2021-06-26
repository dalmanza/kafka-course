package com.dan.dev.kafka.consumer.multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DevConsumerThread extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(DevConsumerThread.class);
  private final KafkaConsumer<String, String> consumer;
  private final AtomicBoolean isClosed = new AtomicBoolean(Boolean.FALSE);
  private static final String TOPIC_NAME = "dan-kafka-01";

  public DevConsumerThread(final KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;
  }

  @Override
  public void run() {
    consumer.subscribe(Arrays.asList(TOPIC_NAME));

    try {
      while (!isClosed.get()) {
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
    } catch (WakeupException e) {
      if (!isClosed.get()) {
        throw e;
      }
    } finally {
      consumer.close();

    }
  }

  public void shutdown() {
    isClosed.set(Boolean.TRUE);
    consumer.wakeup();
  }
}
