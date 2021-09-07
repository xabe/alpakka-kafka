package com.xabe.alpakka.kafka.integration;

import groovy.lang.Tuple2;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Properties;
import java.util.function.BiPredicate;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumer<T extends SpecificRecord> {

  private final Consumer<T> consumer;

  private final BiPredicate<T, Class> predicate;

  public KafkaConsumer(final String topic, final BiPredicate<T, Class> predicate) {
    this.predicate = predicate;
    final Properties properties = new Properties();
    // normal consumer
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("enable.auto.commit", "false");
    properties.setProperty("auto.offset.reset", "latest");
    properties.setProperty("client.id", topic + "-test");
    properties.setProperty("group.id", topic + "-test");

    // avro part
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
    properties.setProperty("use.latest.version", "true");
    properties.setProperty("auto.register.schemas", "false");
    properties.setProperty("specific.avro.reader", "true");
    this.consumer = new Consumer<T>(properties, topic);
  }

  public void before() {
    this.consumer.clear();
  }

  public void close() {
    this.consumer.stop();
  }

  public Tuple2<String, T> expectMessagePipe(final Class payloadClass, final long milliseconds)
      throws InterruptedException {
    final Tuple2<String, T> message = this.consumer.poll(milliseconds);
    if (message == null) {
      throw new RuntimeException("An exception happened while polling the queue for " + payloadClass.getName());
    }
    if (this.predicate.negate().test(message.getV2(), payloadClass)) {
      throw new AssertionError("payload cant be casted");
    }
    return message;
  }
}
