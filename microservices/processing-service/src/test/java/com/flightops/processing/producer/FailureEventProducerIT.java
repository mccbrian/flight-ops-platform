package com.flightops.processing.producer;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.exception.PublishFailureEventException;
import com.flightops.processing.utility.CamelCaseFormatter;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full-context integration test for {@link FailureEventProducer}.
 * <p>
 * A real Kafka broker (Testcontainers) is used so the producer's Avro serialization and Kafka publishing behavior are
 * exercised end to end. A disposable Postgres instance is also started because this test boots the complete Spring context
 * (Flyway migrations and the JDBC repositories run regardless of what this test actually exercises). Kafka listener
 * auto-startup is disabled so the application's own {@code @KafkaListener} consumers don't try to connect and consume
 * during this test.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "server.port=0",
                "spring.data.redis.host=localhost",
                "spring.data.redis.port=6379",
                "spring.kafka.listener.auto-startup=false"
        }
)
@Testcontainers
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Failure Event Producer")
class FailureEventProducerIT {

    private static final String RETRY_TOPIC = "flight-ops.ingestion.retry.v1";

    private static final String DLQ_TOPIC = "flight-ops.ingestion.dlq.v1";

    private static final String SCHEMA_REGISTRY_URL = "mock://flight-ops-test";

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");

    @Autowired
    private FailureEventProducer producer;

    @Autowired
    private KafkaTemplate<String, FailedEvent> kafkaTemplate;

    private Consumer<String, byte[]> consumer;

    private KafkaAvroDeserializer avroDeserializer;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);

        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);
    }

    @BeforeEach
    void setUp() {

        Map<String, Object> properties =
                KafkaTestUtils.consumerProps(
                        kafka.getBootstrapServers(),
                        "failure-event-producer-test",
                        false
                );

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(RETRY_TOPIC, DLQ_TOPIC));

        avroDeserializer = new KafkaAvroDeserializer();

        Map<String, Object> deserializerConfig = new HashMap<>();
        deserializerConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        deserializerConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        avroDeserializer.configure(deserializerConfig, false);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }

        if (avroDeserializer != null) {
            avroDeserializer.close();
        }
    }

    @Test
    void shouldPublishFailedEventToRetryTopic() {

        FailedEvent expected = createFailedEvent();

        producer.sendToRetry(expected);

        ConsumerRecord<String, byte[]> record = pollForRecord(RETRY_TOPIC);

        assertNotNull(record);
        assertEquals(expected.getAggregateId(), record.key());

        FailedEvent actual = deserialize(record);

        assertFailedEventMatches(expected, actual);
    }

    @Test
    void shouldPublishFailedEventToDlqTopic() {

        FailedEvent expected = createFailedEvent();

        producer.sendToDlq(expected);

        ConsumerRecord<String, byte[]> record = pollForRecord(DLQ_TOPIC);

        assertNotNull(record);
        assertEquals(expected.getAggregateId(), record.key());

        FailedEvent actual = deserialize(record);

        assertFailedEventMatches(expected, actual);
    }

    @Test
    void shouldThrowPublishFailureEventExceptionWhenTopicIsInvalid() {

        FailureEventProducer producerWithInvalidTopic = new FailureEventProducer(kafkaTemplate);
        ReflectionTestUtils.setField(producerWithInvalidTopic, "retryTopic", "");

        FailedEvent event = createFailedEvent();

        PublishFailureEventException exception = assertThrows(
                PublishFailureEventException.class,
                () -> producerWithInvalidTopic.sendToRetry(event)
        );

        assertTrue(exception.getMessage().contains(event.getOriginalEventId()));
        assertInstanceOf(KafkaException.class, exception.getCause());
    }


    private void assertFailedEventMatches(FailedEvent expected, FailedEvent actual) {

        assertEquals(expected.getOriginalEventId(), actual.getOriginalEventId());

        assertEquals(expected.getOriginalEventType(), actual.getOriginalEventType());

        assertEquals(expected.getAggregateId(), actual.getAggregateId());

        assertEquals(expected.getCorrelationId(), actual.getCorrelationId());

        assertEquals(expected.getFailureType(), actual.getFailureType());

        assertEquals(expected.getReason(), actual.getReason());

        assertEquals(expected.getAttemptCount(), actual.getAttemptCount());

        assertEquals(expected.getMaxAttempts(), actual.getMaxAttempts());

        assertEquals(expected.getErrorCodes(), actual.getErrorCodes());
    }

    private FailedEvent deserialize(ConsumerRecord<String, byte[]> record) {
        return (FailedEvent) avroDeserializer.deserialize(record.topic(), record.value());
    }

    private ConsumerRecord<String, byte[]> pollForRecord(String topic) {

        Instant deadline = Instant.now().plusSeconds(10);

        while (Instant.now().isBefore(deadline)) {

            var records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, byte[]> record : records) {
                if (record.topic().equals(topic)) {
                    return record;
                }
            }
        }

        fail("No Kafka record received from topic: " + topic);
        return null;
    }

    private FailedEvent createFailedEvent() {

        return FailedEvent.newBuilder()
                .setOriginalEventId(UUID.randomUUID().toString())
                .setOriginalEventType("FLIGHT_OPERATION_EVENT")
                .setAggregateId("1001")
                .setCorrelationId(UUID.randomUUID().toString())
                .setFailureType("RETRYABLE")
                .setErrorCodes(List.of("TEMPORARY_FAILURE"))
                .setReason("Temporary processing failure")
                .setRawPayload("{\"eventId\":\"test-event\"}")
                .setAttemptCount(1)
                .setMaxAttempts(3)
                .setFailedAt(Instant.now())
                .build();
    }

}