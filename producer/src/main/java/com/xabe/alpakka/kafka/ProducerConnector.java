package com.xabe.alpakka.kafka;

import akka.Done;
import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import com.xabe.avro.v1.Car;
import com.xabe.avro.v1.CarCreated;
import com.xabe.avro.v1.MessageEnvelope;
import com.xabe.avro.v1.Metadata;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerConnector {

  private final ActorSystem actorSystem;

  private final ProducerSettings<String, Object> producerSettings;

  private final String topic;

  public ProducerConnector(final String topic) {
    this.topic = topic;
    this.actorSystem = ActorSystem.create("producerApp");
    final Map<String, Object> kafkaAvroSerDeConfig = new HashMap<>();
    kafkaAvroSerDeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
    kafkaAvroSerDeConfig.put("use.latest.version", "true");
    kafkaAvroSerDeConfig.put("auto.register.schemas", "false");
    final KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
    kafkaAvroSerializer.configure(kafkaAvroSerDeConfig, false);
    final Serializer<Object> serializer = kafkaAvroSerializer;
    final Config config = this.actorSystem.settings().config().getConfig("akka.kafka.producer");
    this.producerSettings =
        ProducerSettings.create(config, new StringSerializer(), serializer).withBootstrapServers("localhost:9092");
    CoordinatedShutdown.get(this.actorSystem)
        .addJvmShutdownHook(() -> System.out.println("custom JVM shutdown hook..."));
  }

  public CompletionStage<Done> sendEvent(final String id, final String name) {
    final Car car = Car.newBuilder().setId(id).setName(name).build();
    final CarCreated carCreated = CarCreated.newBuilder().setCar(car).setSentAt(Instant.now().toEpochMilli()).build();
    final MessageEnvelope messageEnvelope = MessageEnvelope.newBuilder().setMetadata(this.createMetaData()).setPayload(carCreated).build();
    return Source.single(new ProducerRecord<String, Object>(this.topic, car.getId(), messageEnvelope))
        .runWith(Producer.plainSink(this.producerSettings), this.actorSystem);
  }

  private Metadata createMetaData() {
    return Metadata.newBuilder().setDomain("car").setName("car").setAction("create").setVersion("vTest")
        .setTimestamp(DateTimeFormatter.ISO_DATE_TIME.format(OffsetDateTime.now())).build();
  }

  public void close() {
    this.actorSystem.terminate();
  }
}