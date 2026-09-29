package com.flightops.processing.service;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.processing.domain.ProcessedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.repository.FlightOperationStatusRepository;
import com.flightops.processing.repository.ProcessedEventRepository;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "server.port=0",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "spring.kafka.consumer.properties.schema.registry.url=http://localhost:8085",
                "spring.data.redis.host=localhost",
                "spring.data.redis.port=6379",
                "spring.kafka.listener.auto-startup=false",
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
@DisplayName("Flight Operation Processing Service Integration")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationProcessingServiceIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");

    @Autowired
    private FlightOperationProcessingService service;

    @Autowired
    private FlightOperationStatusRepository statusRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private int flightId;

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();
        statusRepository.deleteAll();

        jdbcTemplate.update("DELETE FROM bookings.flights WHERE flight_id = 1");

        flightId = insertFlight(
                Instant.now().plusSeconds(3600),
                Instant.now().plusSeconds(7200)
        );
    }

    @Test
    void shouldPersistFlightOperationStatusAndProcessedEventForValidEvent() {

        Instant eventTime = Instant.now().minusSeconds(30);

        EventEnvelopeJson envelope = createEnvelope(eventTime);

        FlightOperationEvent event = createEvent(eventTime, "DELAYED", 15);

        service.process(envelope, event);

        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();

        assertEquals(flightId, status.getFlightId());
        assertEquals("DELAY", status.getOperationType());
        assertEquals("DELAYED", status.getStatus());
        assertEquals("A12", status.getGate());
        assertEquals(15, status.getDelayMinutes());
        assertEquals("WEATHER", status.getReason());
        assertEquals(eventTime.truncatedTo(ChronoUnit.MILLIS), status.getLastEventTime());
        assertNotNull(status.getUpdatedAt());

        ProcessedEvent processedEvent = processedEventRepository.findById(envelope.eventId()).orElseThrow();

        assertEquals(envelope.eventId(), processedEvent.getEventId());
        assertEquals(envelope.eventType().name(), processedEvent.getEventType());
        assertEquals(envelope.aggregateId(), processedEvent.getAggregateId());
        assertNotNull(processedEvent.getProcessedAt());
    }

    @Test
    void shouldUpdateExistingFlightOperationStatusForNewerEvent() {

        Instant firstEventTime = Instant.now().minusSeconds(120);
        Instant secondEventTime = Instant.now().minusSeconds(30);

        EventEnvelopeJson firstEnvelope = createEnvelope(firstEventTime);

        FlightOperationEvent firstEvent = createEvent(firstEventTime, "DELAYED", 10);

        service.process(firstEnvelope, firstEvent);

        EventEnvelopeJson secondEnvelope = createEnvelope(secondEventTime);

        FlightOperationEvent secondEvent = createEvent(secondEventTime, "DELAYED", 25);

        service.process(secondEnvelope, secondEvent);

        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();

        assertEquals("DELAYED", status.getStatus());
        assertEquals(25, status.getDelayMinutes());
        assertEquals(secondEventTime.truncatedTo(ChronoUnit.MILLIS), status.getLastEventTime());

        assertTrue(processedEventRepository.existsById(firstEnvelope.eventId()));
        assertTrue(processedEventRepository.existsById(secondEnvelope.eventId()));
    }

    @Test
    void shouldNotOverwriteExistingStatusForOlderEvent() {

        Instant newerEventTime = Instant.now().minusSeconds(30);
        Instant olderEventTime = Instant.now().minusSeconds(120);

        EventEnvelopeJson newerEnvelope = createEnvelope(newerEventTime);

        FlightOperationEvent newerEvent = createEvent(newerEventTime, "DELAYED", 25);

        service.process(newerEnvelope, newerEvent);

        EventEnvelopeJson olderEnvelope = createEnvelope(olderEventTime);

        FlightOperationEvent olderEvent = createEvent(olderEventTime, "ON_TIME", 0);

        service.process(olderEnvelope, olderEvent);

        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();

        assertEquals("DELAYED", status.getStatus());
        assertEquals(25, status.getDelayMinutes());
        assertEquals(newerEventTime.truncatedTo(ChronoUnit.MILLIS), status.getLastEventTime());

        assertTrue(processedEventRepository.existsById(olderEnvelope.eventId()));
    }

    @Test
    void shouldIgnoreDuplicateEventWithoutChangingFlightStatus() {

        Instant eventTime = Instant.now().minusSeconds(30);

        EventEnvelopeJson envelope = createEnvelope(eventTime);

        FlightOperationEvent event = createEvent(eventTime, "DELAYED", 20);

        service.process(envelope, event);

        FlightOperationStatus initialStatus = statusRepository.findById(flightId).orElseThrow();

        Instant originalEventTime = initialStatus.getLastEventTime();

        service.process(envelope, event);

        FlightOperationStatus statusAfterDuplicate = statusRepository.findById(flightId).orElseThrow();

        assertEquals("DELAYED", statusAfterDuplicate.getStatus());
        assertEquals(20, statusAfterDuplicate.getDelayMinutes());
        assertEquals(originalEventTime, statusAfterDuplicate.getLastEventTime());

        assertEquals(1, processedEventRepository.count());
    }

    @Test
    void shouldRejectEventWhenFlightDoesNotExist() {

        int nonexistentFlightId = flightId + 9999;

        Instant eventTime = Instant.now().minusSeconds(30);

        EventEnvelopeJson envelope = createEnvelope(nonexistentFlightId, eventTime);

        FlightOperationEvent event =
                createEvent(
                        nonexistentFlightId,
                        eventTime,
                        "DELAYED",
                        10
                );

        assertThrows(FlightOperationValidationException.class, () -> service.process(envelope, event));

        assertEquals(0, processedEventRepository.count());

        assertFalse(statusRepository.existsById(nonexistentFlightId));
    }

    @Test
    void shouldRejectEventWhenEventTimeIsTooFarInTheFuture() {

        Instant futureEventTime =
                Instant.now().plusSeconds(60);

        EventEnvelopeJson envelope =
                createEnvelope(futureEventTime);

        FlightOperationEvent event =
                createEvent(
                        flightId,
                        futureEventTime,
                        "DELAYED",
                        10
                );

        FlightOperationValidationException exception =
                assertThrows(
                        FlightOperationValidationException.class,
                        () -> service.process(envelope, event)
                );

        assertTrue(
                exception.errors()
                        .stream()
                        .anyMatch(error ->
                                error.code()
                                        .equals("EVENT_TIME_CLOCK_SKEW_TOO_LARGE")
                        )
        );

        assertEquals(0, processedEventRepository.count());
    }

    @Test
    void shouldPersistStatusWithoutChangingItWhenOlderEventIsProcessed() {

        Instant newerEventTime = Instant.now().minusSeconds(60);
        Instant olderEventTime = Instant.now().minusSeconds(120);

        EventEnvelopeJson newerEnvelope = createEnvelope(newerEventTime);

        FlightOperationEvent newerEvent = createEvent(newerEventTime, "DELAYED", 30);

        service.process(newerEnvelope, newerEvent);

        EventEnvelopeJson olderEnvelope = createEnvelope(olderEventTime);

        FlightOperationEvent olderEvent = createEvent(olderEventTime, "ON_TIME", 0);

        service.process(olderEnvelope, olderEvent);

        FlightOperationStatus status = statusRepository.findById(flightId).orElseThrow();

        assertEquals("DELAYED", status.getStatus());
        assertEquals(30, status.getDelayMinutes());
        assertEquals(newerEventTime.truncatedTo(ChronoUnit.MILLIS), status.getLastEventTime());

        assertEquals(2, processedEventRepository.count());
    }

    private EventEnvelopeJson createEnvelope(Instant eventTime) {
        return createEnvelope(flightId, eventTime);
    }

    private EventEnvelopeJson createEnvelope(
            int targetFlightId,
            Instant eventTime
    ) {
        return new EventEnvelopeJson(
                UUID.randomUUID(),
                EventType.FLIGHT_OPERATION_EVENT,
                String.valueOf(targetFlightId),
                UUID.randomUUID().toString(),
                eventTime,
                null
        );
    }

    private FlightOperationEvent createEvent(
            Instant eventTime,
            String status,
            int delayMinutes
    ) {
        return createEvent(
                flightId,
                eventTime,
                status,
                delayMinutes
        );
    }

    private FlightOperationEvent createEvent(
            int targetFlightId,
            Instant eventTime,
            String status,
            int delayMinutes
    ) {
        return FlightOperationEvent.newBuilder()
                .setFlightId(targetFlightId)
                .setOperationType("DELAY")
                .setStatus(status)
                .setGate("A12")
                .setDelayMinutes(delayMinutes)
                .setReason("WEATHER")
                .setEventTime(eventTime)
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