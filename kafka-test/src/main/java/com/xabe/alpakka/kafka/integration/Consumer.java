package com.xabe.alpakka.kafka.integration;

import groovy.lang.Tuple2;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer<T extends SpecificRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

  private final BlockingQueue<Tuple2<String, T>> messageKafka;

  private final TopicPartition topicPartition;

  private final org.apache.kafka.clients.consumer.Consumer<String, T> consumer;

  private final ExecutorService executor;

  private final AtomicBoolean start = new AtomicBoolean(true);

  public Consumer(final Properties properties, final String topic) {
    this.messageKafka = new ArrayBlockingQueue<>(100);
    this.topicPartition = new TopicPartition(topic, 0);
    this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
    this.consumer.assign(Collections.singleton(this.topicPartition));
    this.consumer.seekToEnd(Collections.singleton(this.topicPartition));
    this.executor = Executors.newSingleThreadExecutor();

    LOGGER.info("Waiting for data...");

    this.executor.submit(() -> {
      while (this.start.get()) {
        LOGGER.info("Polling...");
        final ConsumerRecords<String, T> records = this.consumer.poll(Duration.ofMillis(250));

        for (final ConsumerRecord<String, T> record : records) {
          try {
            this.messageKafka.put(Tuple2.tuple(record.key(), record.value()));
            LOGGER.info("Received event key: {} message:{}", record.key(), record.value());
          } catch (final Exception e) {
            LOGGER.error("Error Received event : {}", e.getMessage(), e);
          }
        }
        this.consumer.commitSync();
      }
      this.consumer.endOffsets(Collections.singleton(this.topicPartition));
      this.consumer.close();
    });
  }

  public void stop() {
    this.start.set(false);
    this.consumer.wakeup();
    this.executor.shutdown();
  }

  public void clear() {
    this.messageKafka.clear();
  }

  public Tuple2<String, T> poll(final long milliseconds) throws InterruptedException {
    return this.messageKafka.poll(milliseconds, TimeUnit.MILLISECONDS);
  }
}
