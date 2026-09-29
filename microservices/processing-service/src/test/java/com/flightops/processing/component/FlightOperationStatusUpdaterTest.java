package com.flightops.processing.component;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Flight Operation Status Updater")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationStatusUpdaterTest {

    private FlightOperationStatusUpdater updater;

    @BeforeEach
    void setUp() {
        updater = new FlightOperationStatusUpdater();
    }

    @Test
    void shouldApplyTheEventWhenItIsNewerThanTheCurrentState() {
        Instant currentEventTime = Instant.parse("2026-08-18T12:00:00Z");
        Instant incomingEventTime = Instant.parse("2026-08-18T13:00:00Z");

        FlightOperationStatus status = createStatus(currentEventTime);

        FlightOperationEvent event = createEvent(incomingEventTime);

        boolean applied = updater.apply(status, event);

        assertTrue(applied);

        assertEquals(
                event.getOperationType(),
                status.getOperationType()
        );
        assertEquals(
                event.getStatus(),
                status.getStatus()
        );
        assertEquals(
                event.getGate(),
                status.getGate()
        );
        assertEquals(
                event.getDelayMinutes(),
                status.getDelayMinutes()
        );
        assertEquals(
                event.getReason(),
                status.getReason()
        );
        assertEquals(
                event.getEventTime(),
                status.getLastEventTime()
        );
    }

    @Test
    void shouldIgnoreTheEventWhenItIsOlderThanTheCurrentState() {
        Instant currentEventTime = Instant.parse("2026-08-18T13:00:00Z");
        Instant incomingEventTime = Instant.parse("2026-08-18T12:00:00Z");

        FlightOperationStatus status = createStatus(currentEventTime);

        FlightOperationEvent event = createEvent(incomingEventTime);

        boolean applied = updater.apply(status, event);

        assertFalse(applied);

        assertEquals(
                currentEventTime,
                status.getLastEventTime()
        );
    }

    @Test
    void shouldIgnoreTheEventWhenItHasTheSameEventTimeAsTheCurrentState() {
        Instant eventTime = Instant.parse("2026-08-18T13:00:00Z");

        FlightOperationStatus status = createStatus(eventTime);

        FlightOperationEvent event = createEvent(eventTime);

        boolean applied = updater.apply(status, event);

        assertFalse(applied);

        assertEquals(
                eventTime,
                status.getLastEventTime()
        );
    }

    private FlightOperationStatus createStatus(Instant lastEventTime) {
        FlightOperationStatus status =
                FlightOperationStatus.initialize(1001);

        status.setOperationType("DEPARTURE");
        status.setStatus("ON_TIME");
        status.setGate("B10");
        status.setDelayMinutes(0);
        status.setReason("Scheduled");
        status.setLastEventTime(lastEventTime);

        return status;
    }

    private FlightOperationEvent createEvent(Instant eventTime) {
        return FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("ARRIVAL")
                .setStatus("DELAYED")
                .setGate("A12")
                .setDelayMinutes(30)
                .setReason("Weather")
                .setEventTime(eventTime)
                .build();
    }
}