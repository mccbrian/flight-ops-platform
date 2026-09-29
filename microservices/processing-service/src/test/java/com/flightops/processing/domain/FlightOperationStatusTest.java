package com.flightops.processing.domain;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Flight Operation Status")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationStatusTest {

    @Test
    void shouldInitializeFlightOperationStatus() {
        Integer flightId = 1001;

        FlightOperationStatus result =
                FlightOperationStatus.initialize(flightId);

        assertEquals(flightId, result.getFlightId());
        assertNotNull(result.getUpdatedAt());
    }

    @Test
    void shouldReturnFalseWhenNoPreviousEventExists() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        assertFalse(
                status.hasNewerOrSameEventThan(Instant.now())
        );
    }

    @Test
    void shouldReturnFalseWhenEventTimeIsNull() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        status.setLastEventTime(Instant.now());

        assertFalse(
                status.hasNewerOrSameEventThan(null)
        );
    }

    @Test
    void shouldReturnTrueWhenExistingEventIsNewer() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        Instant existingEventTime = Instant.parse("2026-08-18T10:00:00Z");
        Instant incomingEventTime = Instant.parse("2026-08-18T09:00:00Z");

        status.setLastEventTime(existingEventTime);

        assertTrue(
                status.hasNewerOrSameEventThan(incomingEventTime)
        );
    }

    @Test
    void shouldReturnTrueWhenEventTimesAreEqual() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        Instant eventTime = Instant.parse("2026-08-18T10:00:00Z");

        status.setLastEventTime(eventTime);

        assertTrue(
                status.hasNewerOrSameEventThan(eventTime)
        );
    }

    @Test
    void shouldReturnFalseWhenIncomingEventIsNewer() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        Instant existingEventTime = Instant.parse("2026-08-18T09:00:00Z");
        Instant incomingEventTime = Instant.parse("2026-08-18T10:00:00Z");

        status.setLastEventTime(existingEventTime);

        assertFalse(
                status.hasNewerOrSameEventThan(incomingEventTime)
        );
    }

    @Test
    void shouldApplyFlightOperationSnapshot() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        Instant eventTime = Instant.parse("2026-08-18T10:00:00Z");

        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("DEPARTED")
                .setStatus("ON_TIME")
                .setGate("A10")
                .setDelayMinutes(0)
                .setReason("Scheduled departure")
                .setEventTime(eventTime)
                .build();

        status.applySnapshot(event);

        assertEquals("DEPARTED", status.getOperationType());
        assertEquals("ON_TIME", status.getStatus());
        assertEquals("A10", status.getGate());
        assertEquals(0, status.getDelayMinutes());
        assertEquals("Scheduled departure", status.getReason());
        assertEquals(eventTime, status.getLastEventTime());
        assertNotNull(status.getUpdatedAt());
    }

    @Test
    void shouldUpdateSnapshotWhenApplyingNewerEvent() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        Instant originalUpdatedAt = status.getUpdatedAt();

        Instant eventTime = Instant.parse("2026-08-18T10:00:00Z");

        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("ARRIVED")
                .setStatus("DELAYED")
                .setGate("B20")
                .setDelayMinutes(15)
                .setReason("Weather delay")
                .setEventTime(eventTime)
                .build();

        status.applySnapshot(event);

        assertEquals("ARRIVED", status.getOperationType());
        assertEquals("DELAYED", status.getStatus());
        assertEquals("B20", status.getGate());
        assertEquals(15, status.getDelayMinutes());
        assertEquals("Weather delay", status.getReason());
        assertEquals(eventTime, status.getLastEventTime());

        assertNotNull(status.getUpdatedAt());
        assertFalse(status.getUpdatedAt().isBefore(originalUpdatedAt));
    }

    @Test
    void shouldBeEqualWhenFlightIdsAreEqual() {
        FlightOperationStatus first =
                FlightOperationStatus.initialize(1001);

        FlightOperationStatus second =
                FlightOperationStatus.initialize(1001);

        assertEquals(first, second);
    }

    @Test
    void shouldNotBeEqualWhenFlightIdsAreDifferent() {
        FlightOperationStatus first =
                FlightOperationStatus.initialize(1001);

        FlightOperationStatus second =
                FlightOperationStatus.initialize(1002);

        assertNotEquals(first, second);
    }

    @Test
    void shouldNotBeEqualToNull() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        assertNotEquals(null, status);
    }

    @Test
    void shouldBeEqualToItself() {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        assertEquals(status, status);
    }

    @Test
    void shouldHaveSameHashCodeWhenFlightIdsAreEqual() {
        FlightOperationStatus first =
                FlightOperationStatus.initialize(1001);

        FlightOperationStatus second =
                FlightOperationStatus.initialize(1001);

        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void shouldHaveDifferentHashCodesWhenFlightIdsAreDifferent() {
        FlightOperationStatus first =
                FlightOperationStatus.initialize(1001);

        FlightOperationStatus second =
                FlightOperationStatus.initialize(1002);

        assertNotEquals(first.hashCode(), second.hashCode());
    }
}