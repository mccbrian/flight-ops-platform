package com.flightops.ingestion.producer;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.ingestion.utility.CamelCaseFormatter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Testcontainers
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Flight Operation Producer")
class FlightOperationProducerIT {

    private static final String TOPIC = "flight-ops.ingestion.v1";
    private static final String SCHEMA_REGISTRY_URL = "mock://flight-ops-test";

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Autowired
    private FlightOperationProducer producer;

    private Consumer<String, byte[]> consumer;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);
    }

    @BeforeEach
    void setUp() {
        Map<String, Object> properties =
                KafkaTestUtils.consumerProps(
                        kafka.getBootstrapServers(),
                        "flight-operation-producer-test",
                        false
                );

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(java.util.List.of(TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void shouldPublishFlightOperationEventToKafka() {
        FlightOperationEnvelope envelope = createEnvelope();

        producer.publish(envelope).join();

        ConsumerRecord<String, byte[]> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(10));

        assertNotNull(record);

        assertEquals(envelope.getAggregateId(), record.key());

        assertNotNull(record.value());
        assertNotNull(record.value());
        assertNotNull(record.headers());

        Header correlationIdHeader = record.headers().lastHeader("X-Correlation-Id");

        Header eventIdHeader = record.headers().lastHeader("X-Event-Id");

        assertNotNull(correlationIdHeader);
        assertNotNull(eventIdHeader);

        assertEquals(
                envelope.getCorrelationId(),
                new String(
                        correlationIdHeader.value(),
                        StandardCharsets.UTF_8
                )
        );

        assertEquals(
                envelope.getEventId(),
                new String(
                        eventIdHeader.value(),
                        StandardCharsets.UTF_8
                )
        );
    }

    private FlightOperationEnvelope createEnvelope() {
        FlightOperationEvent payload = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("ARRIVAL")
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(Instant.now())
                .build();

        return FlightOperationEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setAggregateId("1001")
                .setEventType("FLIGHT_OPERATION_EVENT")
                .setOccurredAt(Instant.now())
                .setPayload(payload)
                .build();
    }
}