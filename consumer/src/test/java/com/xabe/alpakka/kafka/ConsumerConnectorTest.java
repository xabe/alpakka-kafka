package com.xabe.alpakka.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import akka.Done;
import akka.kafka.javadsl.Consumer.DrainingControl;
import com.xabe.alpakka.kafka.integration.KafkaProducer;
import com.xabe.alpakka.kafka.integration.UrlUtil;
import com.xabe.avro.v1.Car;
import com.xabe.avro.v1.CarCreated;
import com.xabe.avro.v1.MessageEnvelope;
import com.xabe.avro.v1.Metadata;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import kong.unirest.Unirest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public class ConsumerConnectorTest {

  private static KafkaProducer<MessageEnvelope> kafkaProducer;

  private static ConsumerConnector consumerConnector;

  private static final Executor ec = Executors.newSingleThreadExecutor();

  @BeforeAll
  public static void init() throws IOException {

    Unirest.post(UrlUtil.getInstance().getUrlSchemaRegistry()).header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .body(Map.of("schema", MessageEnvelope.getClassSchema().toString())).asJson();
    Unirest.put(UrlUtil.getInstance().getUrlSchemaRegistryCompatibility()).header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .body(Map.of("compatibility", "Forward")).asJson();

    kafkaProducer = new KafkaProducer<>("cars.v1");
    consumerConnector = new ConsumerConnector("cars.v1");
  }

  @AfterAll
  public static void end() {
    kafkaProducer.close();
    consumerConnector.close();
  }

  @Test
  @Order(1)
  public void shouldGetAsync() throws Exception {
    //Given
    final Car car = Car.newBuilder().setId("Honda").setName("Honda civic ").build();
    final CarCreated carCreated = CarCreated.newBuilder().setCar(car).setSentAt(Instant.now().toEpochMilli()).build();
    final MessageEnvelope messageEnvelope = MessageEnvelope.newBuilder().setMetadata(this.createMetaData()).setPayload(carCreated).build();
    kafkaProducer.send(messageEnvelope, () -> "Honda");

    //When
    final CompletableFuture<MessageEnvelope> result = consumerConnector.getAsyncEvent();

    //Then

    final MessageEnvelope messageEnvelopeResult = result.get(10, TimeUnit.SECONDS);
    assertThat(messageEnvelopeResult, is(notNullValue()));
    assertThat(messageEnvelopeResult.getPayload(), is(notNullValue()));
  }

  @Test
  @Order(2)
  @Disabled
  public void shouldGetTake() throws Exception {
    //Given
    final Car car = Car.newBuilder().setId("mazda").setName("mazda 3").build();
    final CarCreated carCreated = CarCreated.newBuilder().setCar(car).setSentAt(Instant.now().toEpochMilli()).build();
    final MessageEnvelope messageEnvelope = MessageEnvelope.newBuilder().setMetadata(this.createMetaData()).setPayload(carCreated).build();

    //When
    final DrainingControl<List<ConsumerRecord<String, MessageEnvelope>>> control = consumerConnector.getTakeEvent(2);
    kafkaProducer.send(messageEnvelope, () -> "mazda");
    kafkaProducer.send(messageEnvelope, () -> "mazda");

    //Then
    assertThat(control.isShutdown().toCompletableFuture().get(20, TimeUnit.SECONDS), is(Done.getInstance()));
    final List<ConsumerRecord<String, MessageEnvelope>> consumerRecords =
        control.drainAndShutdown(ec).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThat(consumerRecords.size(), is(1));
    assertThat(consumerRecords.get(0).key(), is("mazda"));
    final MessageEnvelope value = consumerRecords.get(0).value();
    assertThat(value, is(notNullValue()));
    assertThat(value.getPayload(), is(notNullValue()));
  }

  private Metadata createMetaData() {
    return Metadata.newBuilder().setDomain("car").setName("car").setAction("create").setVersion("vTest")
        .setTimestamp(DateTimeFormatter.ISO_DATE_TIME.format(OffsetDateTime.now())).build();
  }
}
