package com.xabe.alpakka.kafka;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.kafka.ConsumerMessage.CommittableMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Consumer.Control;
import akka.stream.javadsl.Sink;
import com.xabe.avro.v1.MessageEnvelope;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

public class ConsumerConnector {

  private final ActorSystem actorSystem;

  private final ConsumerSettings<String, MessageEnvelope> consumerSettings;

  private final ConsumerSettings<String, MessageEnvelope> consumerSettingsWithAutoCommit;

  private final String topic;

  public ConsumerConnector(final String topic) {
    this.topic = topic;
    this.actorSystem = ActorSystem.create("consumerApp");
    final Map<String, Object> kafkaAvroSerDeConfig = new HashMap<>();
    kafkaAvroSerDeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
    kafkaAvroSerDeConfig.put("use.latest.version", "true");
    kafkaAvroSerDeConfig.put("auto.register.schemas", "false");
    kafkaAvroSerDeConfig.put("specific.avro.reader", "true");
    final KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig, false);
    final Deserializer deserializer = kafkaAvroDeserializer;

    this.consumerSettings = ConsumerSettings.<String, MessageEnvelope>create(this.actorSystem, new StringDeserializer(), deserializer)
        .withBootstrapServers("localhost:9092")
        .withGroupId("consumer-alpakka")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    this.consumerSettingsWithAutoCommit = this.consumerSettings
        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
  }

  public void close() {
    this.actorSystem.terminate();
  }

  public Consumer.DrainingControl<List<ConsumerRecord<String, MessageEnvelope>>> getTakeEvent(final int size) {
    return Consumer.plainSource(this.consumerSettingsWithAutoCommit, Subscriptions.assignment(new TopicPartition(this.topic, 0)))
        .take(size)
        .toMat(Sink.seq(), Consumer::createDrainingControl)
        .run(this.actorSystem);
  }

  public CompletableFuture<MessageEnvelope> getAsyncEvent() {
    final CompletableFuture<MessageEnvelope> completableFuture = new CompletableFuture<>();
    Consumer.committableSource(this.consumerSettings, Subscriptions.topics(this.topic))
        .mapAsync(1, this.getEvent(completableFuture))
        .to(Sink.ignore())
        .run(this.actorSystem);
    return completableFuture;
  }

  public Control getControl() {
    return Consumer.atMostOnceSource(this.consumerSettings, Subscriptions.topics(this.topic))
        .mapAsync(2, record -> CompletableFuture.completedFuture(""))
        .to(Sink.foreach(it -> System.out.println("Done with " + it)))
        .run(this.actorSystem);
  }

  private Function<CommittableMessage<String, MessageEnvelope>, CompletionStage<Done>> getEvent(
      final CompletableFuture<MessageEnvelope> completableFutureResult) {
    return msg -> {
      //final GenericRecord record = (GenericRecord) msg.record().value();
      //final MessageEnvelope messageEnvelope = (MessageEnvelope) SpecificData.get().deepCopy(MessageEnvelope.SCHEMA$, record);
      //completableFutureResult.complete(messageEnvelope);
      completableFutureResult.complete(msg.record().value());
      final Future<Done> doneFuture = msg.committableOffset().commitInternal();
      final CompletableFuture<Done> completableFuture = new CompletableFuture<>();
      doneFuture.onComplete(tryResponse -> {
        if (tryResponse.isSuccess()) {
          completableFuture.complete(tryResponse.get());
        } else {
          completableFuture.completeExceptionally(tryResponse.failed().get());
        }
        return completableFuture;
      }, ExecutionContext.global());
      return completableFuture;
    };
  }


}
