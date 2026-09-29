package com.flightops.processing.mapper;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.dto.Payload;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("Flight Operation Mapper")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationMapperTest {

    private FlightOperationMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new FlightOperationMapperImpl();
    }

    @Test
    void shouldMapAFlightOperationEnvelopeToAnEventEnvelopeJson() {
        UUID eventId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        String aggregateId = "1001";
        Instant occurredAt = Instant.parse("2026-08-18T12:00:00Z");

        FlightOperationEnvelope source =
                createEnvelope(
                        eventId.toString(),
                        EventType.FLIGHT_OPERATION_EVENT.name(),
                        aggregateId,
                        correlationId.toString(),
                        occurredAt
                );

        EventEnvelopeJson result = mapper.toEventEnvelopeJson(source);

        assertEquals(eventId, result.eventId());
        assertEquals(EventType.FLIGHT_OPERATION_EVENT, result.eventType());
        assertEquals(aggregateId, result.aggregateId());
        assertEquals(correlationId.toString(), result.correlationId());
        assertEquals(occurredAt, result.occurredAt());
    }

    @Test
    void shouldMapTheFlightOperationEventToAPayload() {
        Instant eventTime = Instant.parse("2026-08-18T12:30:00Z");

        FlightOperationEvent source = createEvent(eventTime);

        Payload result = mapper.toPayload(source);

        assertEquals(1001, result.flightId());

        assertEquals(OperationType.ARRIVAL, result.operationType());

        assertEquals("ON_TIME", result.status());

        assertEquals("A12", result.gate());

        assertEquals(0, result.delayMinutes());

        assertEquals("Scheduled", result.reason());

        assertEquals(eventTime, result.eventTime());
    }

    @Test
    void shouldMapTheNestedPayloadWhenMappingAFlightOperationEnvelope() {
        Instant eventTime = Instant.parse("2026-08-18T12:30:00Z");

        FlightOperationEnvelope source =
                createEnvelope(
                        UUID.randomUUID().toString(),
                        EventType.FLIGHT_OPERATION_EVENT.name(),
                        "1001",
                        UUID.randomUUID().toString(),
                        Instant.parse("2026-08-18T12:00:00Z"),
                        createEvent(eventTime)
                );

        EventEnvelopeJson result = mapper.toEventEnvelopeJson(source);

        assertEquals(1001, result.payload().flightId());

        assertEquals(OperationType.ARRIVAL, result.payload().operationType());

        assertEquals("ON_TIME", result.payload().status());

        assertEquals("A12", result.payload().gate());

        assertEquals(0, result.payload().delayMinutes());

        assertEquals("Scheduled", result.payload().reason());

        assertEquals(eventTime, result.payload().eventTime());
    }

    @Test
    void shouldReturnNullWhenMappingANullFlightOperationEnvelope() {
        EventEnvelopeJson result = mapper.toEventEnvelopeJson(null);

        assertNull(result);
    }

    @Test
    void shouldReturnNullWhenMappingANullFlightOperationEvent() {
        Payload result = mapper.toPayload(null);

        assertNull(result);
    }

    @Test
    void shouldReturnNullWhenMappingANullUuidValue() {
        assertNull(mapper.map(null));
    }

    @Test
    void shouldReturnNullWhenMappingANullEventTypeValue() {
        assertNull(mapper.mapEventType(null));
    }

    @Test
    void shouldReturnNullWhenMappingANullOperationTypeValue() {
        assertNull(mapper.mapOperationType(null));
    }

    private FlightOperationEnvelope createEnvelope(
            String eventId,
            String eventType,
            String aggregateId,
            String correlationId,
            Instant occurredAt) {

        return createEnvelope(
                eventId,
                eventType,
                aggregateId,
                correlationId,
                occurredAt,
                createEvent(occurredAt)
        );
    }

    private FlightOperationEnvelope createEnvelope(
            String eventId,
            String eventType,
            String aggregateId,
            String correlationId,
            Instant occurredAt,
            FlightOperationEvent payload) {

        return FlightOperationEnvelope.newBuilder()
                .setEventId(eventId)
                .setEventType(eventType)
                .setAggregateId(aggregateId)
                .setCorrelationId(correlationId)
                .setOccurredAt(occurredAt)
                .setPayload(payload)
                .build();
    }

    private FlightOperationEvent createEvent(Instant eventTime) {
        return FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(OperationType.ARRIVAL.name())
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(eventTime)
                .build();
    }
}