package com.flightops.processing.flow;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.processing.repository.FlightOperationStatusRepository;
import com.flightops.processing.repository.ProcessedEventRepository;
import com.flightops.processing.utility.CamelCaseFormatter;
import com.flightops.processing.validation.FlightOperationValidator;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * End-to-end integration test of the complete processing flow, from a message arriving on the ingestion topic through
 * to its final outcome.
 * <p>
 * Everything in the application runs for real: the {@code @KafkaListener}s, Avro (de)serialization, mapping, the
 * idempotency store (Redis), validation, persistence (Postgres), failure classification and routing, the
 * retry/DLQ producer, and the retry consumer's recovery loop. Kafka, Postgres and Redis are Testcontainers; the schema
 * registry is Confluent's in-memory {@code mock://} implementation.
 * <p>
 * The only test double is a Mockito spy on {@link FlightOperationValidator}, used solely to inject a database failure
 * for the retry scenarios. It stands in for the flight-existence lookup hitting a transient database error: the
 * exception is thrown from inside the {@code @Transactional} processing call, so transaction rollback is exercised too.
 * In every other scenario the spy is a plain passthrough.
 * <p>
 * Scenarios:
 * <ol>
 *     <li>a valid event is processed and persisted, and nothing is routed to retry or the DLQ</li>
 *     <li>a duplicate delivery is detected and processed only once</li>
 *     <li>a non-retryable validation failure goes straight to the DLQ</li>
 *     <li>a transient failure goes to the retry topic, is reprocessed by the retry consumer, and succeeds</li>
 *     <li>a persistent failure is retried until the attempt limit and then lands in the DLQ</li>
 * </ol>
 * The retry and DLQ topics are observed with a plain Kafka consumer of the test's own (the DLQ has no application
 * consumer). Outcomes are awaited on their observable end state rather than fixed sleeps; the only fixed waits are the
 * short windows used to confirm that something did <em>not</em> happen.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "server.port=0",
                "spring.flyway.default-schema=flight_operations",
                "spring.flyway.schemas=flight_operations",
                "spring.datasource.hikari.connection-init-sql=SET search_path TO bookings,flight_operations"
        }
)
@Sql(
        statements = {
                "CREATE SCHEMA IF NOT EXISTS bookings",

                """
                CREATE TABLE IF NOT EXISTS bookings.flights
                        (
                            flight_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
                            route_no TEXT COLLATE pg_catalog."default" NOT NULL,
                            status TEXT COLLATE pg_catalog."default" NOT NULL,
                            scheduled_departure TIMESTAMP WITH TIME ZONE NOT NULL,
                            scheduled_arrival TIMESTAMP WITH TIME ZONE NOT NULL,
                            actual_departure TIMESTAMP WITH TIME ZONE,
                            actual_arrival TIMESTAMP WITH TIME ZONE,
                            CONSTRAINT flights_pkey PRIMARY KEY (flight_id),
                            CONSTRAINT flights_route_no_scheduled_departure_key UNIQUE (route_no, scheduled_departure)
                        );
                """
        },
        executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
)
@Testcontainers
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Processing Flow")
class ProcessingFlowIT {

    private static final String INGESTION_TOPIC = "flight-ops.ingestion.v1";

    private static final String RETRY_TOPIC = "flight-ops.ingestion.retry.v1";

    private static final String DLQ_TOPIC = "flight-ops.ingestion.dlq.v1";

    private static final String SCHEMA_REGISTRY_URL = "mock://flight-ops-flow-test";
    /*
    Not a real flight: bookings.flights ids come from an identity column that starts at 1.
     */
    private static final int UNKNOWN_FLIGHT_ID = 987_654_321;

    private static final String SIMULATED_FAILURE_MESSAGE = "Simulated transient database failure";

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration NEGATIVE_WINDOW = Duration.ofSeconds(2);

    private static final OperationType OPERATION_TYPE = OperationType.DELAY;
    private static final String STATUS = "DELAYED";
    private static final String GATE = "B7";
    private static final int DELAY_MINUTES = 15;
    private static final String REASON = "WEATHER";

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add("spring.kafka.producer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);

        registry.add("spring.kafka.consumer.properties.schema.registry.url", () -> SCHEMA_REGISTRY_URL);

        registry.add("spring.data.redis.host", redis::getHost);

        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @MockitoSpyBean
    private FlightOperationValidator validator;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private FlightOperationStatusRepository statusRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Value("${app.retry.max-attempts}")
    private int maxAttempts;

    private int flightId;

    private Consumer<String, FailedEvent> observer;

    private final List<ObservedFailedEvent> observed = new ArrayList<>();

    @BeforeAll
    static void createTopics() throws Exception {
        /*
        Single partition each, created up front so ordering and offsets are predictable.
         */
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {

            admin.createTopics(List.of(
                    new NewTopic(INGESTION_TOPIC, 1, (short) 1),
                    new NewTopic(RETRY_TOPIC, 1, (short) 1),
                    new NewTopic(DLQ_TOPIC, 1, (short) 1)
            )).all().get(30, TimeUnit.SECONDS);

        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof TopicExistsException)) {
                throw exception;
            }
        }
    }

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();
        statusRepository.deleteAll();

        jdbcTemplate.update("DELETE FROM bookings.flights");

        flightId = insertFlight(
                Instant.now().plusSeconds(3600),
                Instant.now().plusSeconds(7200)
        );

        startObserver();
    }

    @AfterEach
    void tearDown() {
        if (observer != null) {
            observer.close();
        }
    }
    /*
    Scenarios
     */
    @Test
    void shouldProcessValidEventEndToEndAndRouteNothingToRetryOrDlq() {

        UUID eventId = UUID.randomUUID();
        Instant eventTime = eventTime(5);

        Metrics before = readMetrics();

        send(createEnvelope(eventId, flightId, eventTime));

        awaitProcessed(eventId);

        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();

        assertEquals(OPERATION_TYPE.name(), status.getOperationType());
        assertEquals(STATUS, status.getStatus());
        assertEquals(GATE, status.getGate());
        assertEquals(DELAY_MINUTES, status.getDelayMinutes());
        assertEquals(REASON, status.getReason());
        assertEquals(eventTime, status.getLastEventTime());

        assertTrue(processedEventRepository.existsById(eventId));
        awaitProcessingClaimCleared(eventId);

        assertMetricsDelta(before, Metrics.none().withProcessed(1));

        pollFor();
        assertTrue(observedFor(eventId).isEmpty(), "a successful event must not be routed to retry or the DLQ");
    }

    @Test
    void shouldProcessDuplicateDeliveryOnlyOnce() {

        UUID eventId = UUID.randomUUID();
        UUID sentinelEventId = UUID.randomUUID();
        Instant eventTime = eventTime(10);
        Instant sentinelEventTime = eventTime(5);

        Metrics before = readMetrics();

        FlightOperationEnvelope envelope = createEnvelope(eventId, flightId, eventTime);
        /*
        The same event twice, then a distinct sentinel. Everything goes to one partition and is consumed in order,
        so once the sentinel has been processed, the duplicate is guaranteed to have been handled already.
         */
        send(envelope);
        send(envelope);
        send(createEnvelope(sentinelEventId, flightId, sentinelEventTime));

        awaitProcessed(sentinelEventId);

        assertTrue(processedEventRepository.existsById(eventId));
        assertTrue(processedEventRepository.existsById(sentinelEventId));
        assertEquals(2, processedEventRepository.count(), "the duplicate must not create a second processed record");

        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();
        assertEquals(sentinelEventTime, status.getLastEventTime());

        assertMetricsDelta(before, Metrics.none().withProcessed(2).withDuplicate());

        pollFor();
        assertTrue(observedFor(eventId).isEmpty(), "a duplicate is ignored, not routed to retry or the DLQ");
        assertTrue(observedFor(sentinelEventId).isEmpty());
    }

    @Test
    void shouldRouteNonRetryableValidationFailureStraightToDlq() {

        UUID eventId = UUID.randomUUID();

        Metrics before = readMetrics();

        send(createEnvelope(eventId, UNKNOWN_FLIGHT_ID, eventTime(5)));

        awaitObserved(DLQ_TOPIC, eventId, 1);

        FailedEvent dead = observedOn(DLQ_TOPIC, eventId).getFirst();

        assertEquals(eventId.toString(), dead.getOriginalEventId());
        assertEquals(EventType.FLIGHT_OPERATION_EVENT.name(), dead.getOriginalEventType());
        assertEquals(String.valueOf(UNKNOWN_FLIGHT_ID), dead.getAggregateId());
        assertEquals("NON_RETRYABLE", dead.getFailureType());
        assertEquals(List.of("FLIGHT_NOT_FOUND"), dead.getErrorCodes());
        assertEquals(1, dead.getAttemptCount());
        assertEquals(maxAttempts, dead.getMaxAttempts());
        assertTrue(dead.getRawPayload().contains(eventId.toString()), "the original envelope should be preserved");

        assertMetricsDelta(before, Metrics.none().withDlq().withFailed());

        pollFor();
        assertTrue(observedOn(RETRY_TOPIC, eventId).isEmpty(), "a non-retryable failure must not be retried");
        assertEquals(1, observedOn(DLQ_TOPIC, eventId).size());

        assertFalse(statusRepository.existsById(UNKNOWN_FLIGHT_ID));
        assertFalse(processedEventRepository.existsById(eventId));
        assertFalse(hasKey(processedKey(eventId)));
        assertFalse(hasKey(processingKey(eventId)), "the claim must be released when routing to the DLQ");
    }

    @Test
    void shouldRetryTransientFailureAndSucceedOnRecovery() {

        UUID eventId = UUID.randomUUID();
        Instant eventTime = eventTime(5);
        /*
        Fail the first attempt only; every later attempt runs the real validation.
         */
        doThrow(new DataAccessResourceFailureException(SIMULATED_FAILURE_MESSAGE))
                .doCallRealMethod()
                .when(validator).validate(argThat(forFlight(flightId)));

        Metrics before = readMetrics();

        send(createEnvelope(eventId, flightId, eventTime));

        awaitProcessed(eventId);
        awaitObserved(RETRY_TOPIC, eventId, 1);

        FailedEvent retried = observedOn(RETRY_TOPIC, eventId).getFirst();

        assertEquals(eventId.toString(), retried.getOriginalEventId());
        assertEquals(String.valueOf(flightId), retried.getAggregateId());
        assertEquals("RETRYABLE", retried.getFailureType());
        assertTrue(retried.getErrorCodes().isEmpty(), "an unexpected failure carries no validation error codes");
        assertTrue(retried.getReason().contains(SIMULATED_FAILURE_MESSAGE));
        assertEquals(1, retried.getAttemptCount());
        assertEquals(maxAttempts, retried.getMaxAttempts());
        assertTrue(retried.getRawPayload().contains(eventId.toString()), "the original envelope should be preserved");
        /*
        The retry consumer really did reprocess the event, and the second attempt applied it.
         */
        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();
        assertEquals(STATUS, status.getStatus());
        assertEquals(eventTime, status.getLastEventTime());
        assertTrue(processedEventRepository.existsById(eventId));
        awaitProcessingClaimCleared(eventId);

        verify(validator, times(2)).validate(argThat(forFlight(flightId)));

        assertMetricsDelta(before, Metrics.none().withProcessed(1).withRetry(1));

        pollFor();
        assertEquals(1, observedOn(RETRY_TOPIC, eventId).size(), "only the first attempt should have needed a retry");
        assertTrue(observedOn(DLQ_TOPIC, eventId).isEmpty(), "a recovered event must not reach the DLQ");
    }

    @Test
    void shouldRetryPersistentFailureUntilAttemptLimitThenRouteToDlq() {

        UUID eventId = UUID.randomUUID();
        /*
        Every attempt fails.
         */
        doThrow(new DataAccessResourceFailureException(SIMULATED_FAILURE_MESSAGE))
                .when(validator).validate(argThat(forFlight(flightId)));

        Metrics before = readMetrics();

        send(createEnvelope(eventId, flightId, eventTime(5)));

        awaitObserved(DLQ_TOPIC, eventId, 1);
        awaitObserved(RETRY_TOPIC, eventId, maxAttempts - 1);

        pollFor();
        /*
        Attempts 1 .. (max - 1) are each sent to retry; attempt number max is dead-lettered.
         */
        List<Integer> retriedAttempts = observedOn(RETRY_TOPIC, eventId).stream()
                .map(FailedEvent::getAttemptCount)
                .toList();

        assertEquals(IntStream.range(1, maxAttempts).boxed().toList(), retriedAttempts);

        assertEquals(1, observedOn(DLQ_TOPIC, eventId).size());

        FailedEvent dead = observedOn(DLQ_TOPIC, eventId).getFirst();

        assertEquals(eventId.toString(), dead.getOriginalEventId());
        assertEquals("RETRYABLE", dead.getFailureType());
        assertEquals(maxAttempts, dead.getAttemptCount());
        assertEquals(maxAttempts, dead.getMaxAttempts());
        assertTrue(dead.getReason().contains(SIMULATED_FAILURE_MESSAGE));

        verify(validator, times(maxAttempts)).validate(argThat(forFlight(flightId)));

        assertMetricsDelta(before, Metrics.none().withRetry(maxAttempts - 1).withDlq().withFailed());

        assertFalse(statusRepository.existsById(flightId));
        assertFalse(processedEventRepository.existsById(eventId));
        assertFalse(hasKey(processedKey(eventId)));
        assertFalse(hasKey(processingKey(eventId)), "the claim must be released when routing to the DLQ");
    }
    /*
    Sending
     */
    private void send(FlightOperationEnvelope envelope) {
        try {
            kafkaTemplate.send(INGESTION_TOPIC, envelope.getAggregateId(), envelope)
                    .get(15, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending to " + INGESTION_TOPIC, exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failed to send to " + INGESTION_TOPIC, exception);
        }
    }
    /*
    Observing the retry and DLQ topics
     */
    private record ObservedFailedEvent(String topic, FailedEvent event) {
    }

    private void startObserver() {

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        /*
        A fresh group each time, always reading from the beginning; events are filtered by their original event id.
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "processing-flow-observer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        observer = new KafkaConsumer<>(properties);
        observer.subscribe(List.of(RETRY_TOPIC, DLQ_TOPIC));
    }

    private void pollObserver(Duration timeout) {
        for (ConsumerRecord<String, FailedEvent> record : observer.poll(timeout)) {
            observed.add(new ObservedFailedEvent(record.topic(), record.value()));
        }
    }

    private void pollFor() {
        Instant deadline = Instant.now().plus(ProcessingFlowIT.NEGATIVE_WINDOW);

        while (Instant.now().isBefore(deadline)) {
            pollObserver(Duration.ofMillis(200));
        }
    }

    private List<FailedEvent> observedOn(String topic, UUID originalEventId) {
        return observed.stream()
                .filter(entry -> entry.topic().equals(topic))
                .map(ObservedFailedEvent::event)
                .filter(event -> originalEventId.toString().equals(event.getOriginalEventId()))
                .toList();
    }

    private List<FailedEvent> observedFor(UUID originalEventId) {
        return observed.stream()
                .map(ObservedFailedEvent::event)
                .filter(event -> originalEventId.toString().equals(event.getOriginalEventId()))
                .toList();
    }

    private void awaitObserved(String topic, UUID originalEventId, int expectedCount) {
        await(
                () -> {
                    pollObserver(Duration.ofMillis(300));
                    return observedOn(topic, originalEventId).size() >= expectedCount;
                },
                expectedCount + " record(s) for event " + originalEventId + " on " + topic
        );
    }
    /*
    Redis and metrics
     */
    private void awaitProcessed(UUID eventId) {
        await(
                () -> hasKey(processedKey(eventId)),
                "event " + eventId + " to be marked processed"
        );
    }
    /*
    markProcessed() sets the processed key and then deletes the processing key as two separate Redis calls, so the
    processing key can briefly outlive the moment the processed key becomes visible.
     */
    private void awaitProcessingClaimCleared(UUID eventId) {
        await(
                () -> !hasKey(processingKey(eventId)),
                "the processing claim for event " + eventId + " to be cleared"
        );
    }

    private boolean hasKey(String key) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    private String processingKey(UUID eventId) {
        return "processing:event:" + eventId;
    }

    private String processedKey(UUID eventId) {
        return "processed:event:" + eventId;
    }

    /**
     * Counter values, or (via {@link #minus}) the change in them over a scenario. The application's counters are
     * cumulative across the whole test class, so every scenario compares against a snapshot taken before it starts.
     */
    private record Metrics(double processed, double failed, double retry, double dlq, double duplicate) {

        static Metrics none() {
            return new Metrics(0, 0, 0, 0, 0);
        }

        Metrics withProcessed(double value) {
            return new Metrics(value, failed, retry, dlq, duplicate);
        }

        Metrics withFailed() {
            return new Metrics(processed, 1, retry, dlq, duplicate);
        }

        Metrics withRetry(double value) {
            return new Metrics(processed, failed, value, dlq, duplicate);
        }

        Metrics withDlq() {
            return new Metrics(processed, failed, retry, 1, duplicate);
        }

        Metrics withDuplicate() {
            return new Metrics(processed, failed, retry, dlq, 1);
        }

        Metrics minus(Metrics other) {
            return new Metrics(
                    processed - other.processed,
                    failed - other.failed,
                    retry - other.retry,
                    dlq - other.dlq,
                    duplicate - other.duplicate
            );
        }
    }

    private Metrics readMetrics() {
        return new Metrics(
                counter("flight_ops_processed_total"),
                counter("flight_ops_failed_total"),
                counter("flight_ops_retry_total"),
                counter("flight_ops_dlq_total"),
                counter("flight_ops_duplicate_total")
        );
    }

    private double counter(String name) {
        return meterRegistry.get(name).counter().count();
    }

    /**
     * Some counters are incremented just after the step a scenario waits on (e.g. the DLQ counter right after the DLQ
     * record is published), so this allows a moment for them to settle before asserting the exact change.
     */
    private void assertMetricsDelta(Metrics before, Metrics expected) {

        Instant deadline = Instant.now().plus(AWAIT_TIMEOUT);

        while (Instant.now().isBefore(deadline)) {
            if (expected.equals(readMetrics().minus(before))) {
                return;
            }
            sleep();
        }

        assertEquals(expected, readMetrics().minus(before), "Unexpected change in metric counters");
    }
    /*
    Waiting
     */
    private void await(BooleanSupplier condition, String description) {

        Instant deadline = Instant.now().plus(AWAIT_TIMEOUT);

        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep();
        }

        fail("Timed out waiting for " + description);
    }

    private void sleep() {
        try {
            Thread.sleep((long) 100);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            fail("Interrupted while waiting");
        }
    }
    /*
    Fixtures
     */
    private static ArgumentMatcher<FlightOperationEvent> forFlight(int targetFlightId) {
        return event -> event != null && event.getFlightId() == targetFlightId;
    }

    private Instant eventTime(long secondsAgo) {
        /*
        Avro timestamps carry millisecond precision, so truncate to keep equality assertions exact.
         */
        return Instant.now().minusSeconds(secondsAgo).truncatedTo(ChronoUnit.MILLIS);
    }

    private FlightOperationEnvelope createEnvelope(UUID eventId, int targetFlightId, Instant eventTime) {

        return FlightOperationEnvelope.newBuilder()
                .setEventId(eventId.toString())
                .setEventType(EventType.FLIGHT_OPERATION_EVENT.name())
                .setAggregateId(String.valueOf(targetFlightId))
                .setCorrelationId(UUID.randomUUID().toString())
                .setOccurredAt(Instant.now())
                .setPayload(
                        FlightOperationEvent.newBuilder()
                                .setFlightId(targetFlightId)
                                .setOperationType(OPERATION_TYPE.name())
                                .setStatus(STATUS)
                                .setGate(GATE)
                                .setDelayMinutes(DELAY_MINUTES)
                                .setReason(REASON)
                                .setEventTime(eventTime)
                                .build()
                )
                .build();
    }

    private int insertFlight(Instant scheduledDeparture, Instant scheduledArrival) {
        String sql = """
            INSERT INTO bookings.flights (
                route_no,
                status,
                scheduled_departure,
                scheduled_arrival
            )
            VALUES (?, ?, ?, ?)
            RETURNING flight_id
            """;

        return Objects.requireNonNull(
                jdbcTemplate.queryForObject(
                        sql,
                        Integer.class,
                        "AA100",
                        "SCHEDULED",
                        java.sql.Timestamp.from(scheduledDeparture),
                        java.sql.Timestamp.from(scheduledArrival)
                )
        );
    }

}