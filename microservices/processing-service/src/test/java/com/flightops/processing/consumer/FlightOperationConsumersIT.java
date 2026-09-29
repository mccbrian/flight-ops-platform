package com.flightops.processing.consumer;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.service.EventProcessingCoordinator;
import com.flightops.processing.service.FailedEventRecoveryService;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;

/**
 * Integration test for {@link FlightOperationConsumer} and {@link FlightOperationRetryConsumer} running as real
 * {@code @KafkaListener}s against a real Kafka broker (Testcontainers), with real Avro serialization through a mock
 * schema registry.
 * <p>
 * {@link FlightOperationConsumerTest} and {@link FlightOperationRetryConsumerTest} already cover the listener methods'
 * logic by calling them directly with a mocked {@code Acknowledgment}. This class instead verifies what only a live
 * listener container can prove:
 * <ul>
 *     <li>each listener is bound to the topic and consumer group the application configuration names</li>
 *     <li>a real Avro record is deserialized into the listener method's parameter type</li>
 *     <li>{@code spring.kafka.listener.ack-mode=manual} actually supplies a working {@code Acknowledgment}, and calling
 *     it commits the record's offset to the broker</li>
 *     <li>when processing fails, the offset is <em>not</em> committed</li>
 * </ul>
 * The downstream {@link EventProcessingCoordinator} and {@link FailedEventRecoveryService} are replaced with mocks:
 * their behavior is covered elsewhere, and this test is about the listeners in front of them. Unlike the other
 * integration tests in this project, listener auto-startup is deliberately left <em>enabled</em>. A disposable
 * Postgres instance is still started because the complete Spring context is booted (Flyway migrations run regardless
 * of what is being tested); Redis is never touched, so it gets placeholder properties.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "server.port=0",
                "spring.data.redis.host=localhost",
                "spring.data.redis.port=6379"
        }
)
@Testcontainers
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Flight Operation Consumers")
class FlightOperationConsumersIT {

    private static final String INGESTION_TOPIC = "flight-ops.ingestion.v1";

    private static final String RETRY_TOPIC = "flight-ops.ingestion.retry.v1";

    private static final String INGESTION_GROUP = "flight-ops-processing-group";

    private static final String RETRY_GROUP = "flight-ops-retry-group";

    private static final String SCHEMA_REGISTRY_URL = "mock://flight-ops-consumer-test";
    /*
    Manual acks are committed on the container's next poll (milliseconds), so this is far longer than a wrongly
    acknowledged record would need to show up as a committed offset.
     */
    private static final long SETTLE_MILLIS = 3_000;

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);

        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);
    }

    @MockitoBean
    private EventProcessingCoordinator coordinator;

    @MockitoBean
    private FailedEventRecoveryService recoveryService;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeAll
    static void createTopics() throws Exception {
        /*
        Single partition each, created up front so partition assignment and offsets are predictable.
         */
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {

            admin.createTopics(List.of(
                    new NewTopic(INGESTION_TOPIC, 1, (short) 1),
                    new NewTopic(RETRY_TOPIC, 1, (short) 1)
            )).all().get(30, TimeUnit.SECONDS);

        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof TopicExistsException)) {
                throw exception;
            }
        }
    }

    @Test
    void shouldDeserializeMapProcessAndCommitOffsetForIngestionEvent() {

        UUID eventId = UUID.randomUUID();

        AtomicReference<EventEnvelopeJson> receivedEnvelope = new AtomicReference<>();
        AtomicInteger receivedAttemptCount = new AtomicInteger();

        doAnswer(invocation -> {
            EventEnvelopeJson envelope = invocation.getArgument(0);
            if (eventId.equals(envelope.eventId())) {
                int attemptCount = invocation.getArgument(1);
                receivedAttemptCount.set(attemptCount);
                receivedEnvelope.set(envelope);
            }
            return null;
        }).when(coordinator).processEnvelope(any(), anyInt());

        RecordMetadata sent = send(INGESTION_TOPIC, createEnvelope(eventId));

        await(() -> receivedEnvelope.get() != null, "the coordinator to receive the ingestion event");

        EventEnvelopeJson envelope = receivedEnvelope.get();

        assertEquals(eventId, envelope.eventId());
        assertEquals(EventType.FLIGHT_OPERATION_EVENT, envelope.eventType());
        assertEquals("1001", envelope.aggregateId());
        assertEquals("correlation-" + eventId, envelope.correlationId());
        assertNotNull(envelope.payload());
        assertEquals(OperationType.ARRIVAL, envelope.payload().operationType());
        assertEquals(1, receivedAttemptCount.get());

        awaitCommittedOffsetAtLeast(INGESTION_GROUP, topicPartition(sent), sent.offset() + 1);
    }

    @Test
    void shouldNotCommitOffsetWhenIngestionProcessingFails() {

        UUID eventId = UUID.randomUUID();

        AtomicInteger attempts = new AtomicInteger();

        doAnswer(invocation -> {
            EventEnvelopeJson envelope = invocation.getArgument(0);
            if (eventId.equals(envelope.eventId())) {
                attempts.incrementAndGet();
                throw new IllegalStateException("Simulated processing failure");
            }
            return null;
        }).when(coordinator).processEnvelope(any(), anyInt());

        RecordMetadata sent = send(INGESTION_TOPIC, createEnvelope(eventId));

        await(() -> attempts.get() >= 1, "the listener to attempt the failing ingestion event");

        assertOffsetNotCommittedBeyond(INGESTION_GROUP, topicPartition(sent), sent.offset());
    }

    @Test
    void shouldDeserializeRecoverAndCommitOffsetForRetryEvent() {

        String originalEventId = UUID.randomUUID().toString();

        AtomicReference<FailedEvent> receivedEvent = new AtomicReference<>();

        doAnswer(invocation -> {
            FailedEvent failedEvent = invocation.getArgument(0);
            if (originalEventId.equals(failedEvent.getOriginalEventId())) {
                receivedEvent.set(failedEvent);
            }
            return null;
        }).when(recoveryService).recover(any());

        RecordMetadata sent = send(RETRY_TOPIC, createFailedEvent(originalEventId));

        await(() -> receivedEvent.get() != null, "the recovery service to receive the retry event");

        FailedEvent failedEvent = receivedEvent.get();

        assertEquals(originalEventId, failedEvent.getOriginalEventId());
        assertEquals("1001", failedEvent.getAggregateId());
        assertEquals("correlation-" + originalEventId, failedEvent.getCorrelationId());
        assertEquals("RETRYABLE", failedEvent.getFailureType());
        assertEquals(1, failedEvent.getAttemptCount());
        assertEquals(3, failedEvent.getMaxAttempts());

        awaitCommittedOffsetAtLeast(RETRY_GROUP, topicPartition(sent), sent.offset() + 1);
    }

    @Test
    void shouldNotCommitOffsetWhenRetryRecoveryFails() {

        String originalEventId = UUID.randomUUID().toString();

        AtomicInteger attempts = new AtomicInteger();

        doAnswer(invocation -> {
            FailedEvent failedEvent = invocation.getArgument(0);
            if (originalEventId.equals(failedEvent.getOriginalEventId())) {
                attempts.incrementAndGet();
                throw new IllegalStateException("Simulated recovery failure");
            }
            return null;
        }).when(recoveryService).recover(any());

        RecordMetadata sent = send(RETRY_TOPIC, createFailedEvent(originalEventId));

        await(() -> attempts.get() >= 1, "the retry listener to attempt the failing retry event");

        assertOffsetNotCommittedBeyond(RETRY_GROUP, topicPartition(sent), sent.offset());
    }

    private RecordMetadata send(String topic, Object value) {
        try {
            return kafkaTemplate.send(topic, "1001", value)
                    .get(15, TimeUnit.SECONDS)
                    .getRecordMetadata();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending to " + topic, exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failed to send to " + topic, exception);
        }
    }

    private TopicPartition topicPartition(RecordMetadata metadata) {
        return new TopicPartition(metadata.topic(), metadata.partition());
    }

    /**
     * Returns the offset the group has committed for the partition (i.e. the next offset it would read), or
     * {@code null} if the group has never committed one.
     */
    private Long committedOffset(String groupId, TopicPartition partition) {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {

            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);

            OffsetAndMetadata committed = offsets.get(partition);

            return committed == null ? null : committed.offset();

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading committed offsets", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failed to read committed offsets for group " + groupId, exception);
        }
    }

    private void awaitCommittedOffsetAtLeast(String groupId, TopicPartition partition, long expectedOffset) {
        await(
                () -> {
                    Long committed = committedOffset(groupId, partition);
                    return committed != null && committed >= expectedOffset;
                },
                "group " + groupId + " to commit offset >= " + expectedOffset + " on " + partition
        );
    }

    /**
     * Passes only if the group has not committed past {@code failedRecordOffset}, i.e. the failed record is still
     * uncommitted and would be redelivered after a restart or rebalance.
     */
    private void assertOffsetNotCommittedBeyond(String groupId, TopicPartition partition, long failedRecordOffset) {

        sleep(SETTLE_MILLIS);

        Long committed = committedOffset(groupId, partition);

        assertTrue(
                committed == null || committed <= failedRecordOffset,
                "Offset for failed record " + failedRecordOffset + " on " + partition
                        + " was committed (group " + groupId + " committed offset " + committed + ")"
        );
    }

    private void await(BooleanSupplier condition, String description) {

        Instant deadline = Instant.now().plusSeconds(15);

        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(200);
        }

        fail("Timed out waiting for " + description);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            fail("Interrupted while waiting");
        }
    }

    private FlightOperationEnvelope createEnvelope(UUID eventId) {

        return FlightOperationEnvelope.newBuilder()
                .setEventId(eventId.toString())
                .setEventType(EventType.FLIGHT_OPERATION_EVENT.name())
                .setAggregateId("1001")
                .setCorrelationId("correlation-" + eventId)
                .setOccurredAt(Instant.now())
                .setPayload(
                        FlightOperationEvent.newBuilder()
                                .setFlightId(1001)
                                .setOperationType(OperationType.ARRIVAL.name())
                                .setStatus("ON_TIME")
                                .setGate("A10")
                                .setDelayMinutes(0)
                                .setReason("Scheduled arrival")
                                .setEventTime(Instant.now())
                                .build()
                )
                .build();
    }

    private FailedEvent createFailedEvent(String originalEventId) {

        return FailedEvent.newBuilder()
                .setOriginalEventId(originalEventId)
                .setOriginalEventType(EventType.FLIGHT_OPERATION_EVENT.name())
                .setAggregateId("1001")
                .setCorrelationId("correlation-" + originalEventId)
                .setFailureType("RETRYABLE")
                .setErrorCodes(List.of("TEMPORARY_FAILURE"))
                .setReason("Temporary processing failure")
                .setRawPayload("{}")
                .setAttemptCount(1)
                .setMaxAttempts(3)
                .setFailedAt(Instant.now())
                .build();
    }

}