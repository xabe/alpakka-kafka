package com.xabe.alpakka.kafka;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Patterns.$Failure;
import static io.vavr.Patterns.$Success;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import akka.Done;
import com.xabe.alpakka.kafka.integration.KafkaConsumer;
import com.xabe.alpakka.kafka.integration.UrlUtil;
import com.xabe.avro.v1.CarCreated;
import com.xabe.avro.v1.MessageEnvelope;
import groovy.lang.Tuple2;
import io.vavr.control.Try;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import kong.unirest.Unirest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;

@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
public class ProducerConnectorTest {

  public static final int TIMEOUT_MS = 10000;

  public static final int DELAY_MS = 1500;

  public static final int POLL_INTERVAL_MS = 500;

  private static KafkaConsumer<MessageEnvelope> kafkaConsumer;

  private static ProducerConnector producerConnector;

  @BeforeAll
  public static void init() throws IOException {

    Unirest.post(UrlUtil.getInstance().getUrlSchemaRegistry()).header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .body(Map.of("schema", MessageEnvelope.getClassSchema().toString())).asJson();
    Unirest.put(UrlUtil.getInstance().getUrlSchemaRegistryCompatibility()).header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .body(Map.of("compatibility", "Forward")).asJson();

    kafkaConsumer = new KafkaConsumer<>("cars.v1", (message, payloadClass) -> message.getPayload().getClass().equals(payloadClass));
    producerConnector = new ProducerConnector("cars.v1");
  }

  @AfterAll
  public static void end() {
    kafkaConsumer.close();
    producerConnector.close();
  }

  @BeforeEach
  public void before() {
    kafkaConsumer.before();
  }

  @Test
  @Order(1)
  public void shouldSend() throws Exception {
    //Given

    //When
    final CompletionStage<Done> result = producerConnector.sendEvent("mazda", "mazda 3");

    //Then
    Match(Try.of(() -> result.toCompletableFuture().get())).of(
        Case($Success($()), this.processSuccessChannel()),
        Case($Failure($()), this.processFailureChannel())
    );

    Awaitility.await().pollDelay(DELAY_MS, TimeUnit.MILLISECONDS).pollInterval(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .atMost(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> {
          final Tuple2<String, MessageEnvelope> resultConsumer = kafkaConsumer.expectMessagePipe(CarCreated.class, TIMEOUT_MS);
          assertThat(resultConsumer, is(notNullValue()));
          assertThat(resultConsumer.getV1(), is(notNullValue()));
          assertThat(resultConsumer.getV2(), is(notNullValue()));
          final CarCreated carCreated = CarCreated.class.cast(resultConsumer.getV2().getPayload());
          assertThat(carCreated.getCar(), is(notNullValue()));
          assertThat(carCreated.getCar().getId(), is(notNullValue()));
          assertThat(carCreated.getCar().getName(), is(notNullValue()));
          assertThat(carCreated.getSentAt(), is(notNullValue()));
          return true;
        });

  }

  private Function<Done, Done> processSuccessChannel() {
    return done -> {
      System.out.println("Message sent successful");
      return done;
    };
  }

  private Function<Throwable, Throwable> processFailureChannel() {
    return t -> {
      System.out.println("Message not sent. Caused by " + t);
      return t;
    };
  }


}
