package com.flightops.processing.domain;

import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Processed Event")
@DisplayNameGeneration(CamelCaseFormatter.class)
class ProcessedEventTest {

    @Test
    void shouldCreateProcessedEventWithExpectedValues() {
        UUID eventId = UUID.randomUUID();
        String eventType = "FLIGHT_OPERATION_EVENT";
        String aggregateId = "1001";

        ProcessedEvent event =
                new ProcessedEvent(eventId, eventType, aggregateId);

        assertEquals(eventId, event.getEventId());
        assertEquals(eventType, event.getEventType());
        assertEquals(aggregateId, event.getAggregateId());
        assertNotNull(event.getProcessedAt());
    }

    @Test
    void shouldReturnEventIdFromGetId() {
        UUID eventId = UUID.randomUUID();

        ProcessedEvent event = new ProcessedEvent(eventId,"FLIGHT_OPERATION_EVENT","1001");

        assertEquals(eventId, event.getId());
    }

    @Test
    void shouldAlwaysBeConsideredNew() {
        ProcessedEvent event = new ProcessedEvent(UUID.randomUUID(),"FLIGHT_OPERATION_EVENT","1001");

        assertTrue(event.isNew());
    }

    @Test
    void shouldBeEqualWhenEventIdsAreEqual() {
        UUID eventId = UUID.randomUUID();

        ProcessedEvent first = new ProcessedEvent(eventId,"FLIGHT_OPERATION_EVENT","1001");

        ProcessedEvent second = new ProcessedEvent(eventId,"FLIGHT_OPERATION_EVENT","1001");

        assertEquals(first, second);
    }

    @Test
    void shouldNotBeEqualWhenEventIdsAreDifferent() {
        ProcessedEvent first = new ProcessedEvent(UUID.randomUUID(),"FLIGHT_OPERATION_EVENT","1001");

        ProcessedEvent second = new ProcessedEvent(UUID.randomUUID(),"FLIGHT_OPERATION_EVENT","1001");

        assertNotEquals(first, second);
    }

    @Test
    void shouldNotBeEqualToNull() {
        ProcessedEvent event = new ProcessedEvent(UUID.randomUUID(),"FLIGHT_OPERATION_EVENT","1001");

        assertNotEquals(null, event);
    }

    @Test
    void shouldBeEqualToItself() {
        ProcessedEvent event = new ProcessedEvent(UUID.randomUUID(),"FLIGHT_OPERATION_EVENT","1001");

        assertEquals(event, event);
    }

    @Test
    void shouldHaveSameHashCodeWhenEventIdsAreEqual() {
        UUID eventId = UUID.randomUUID();

        ProcessedEvent first = new ProcessedEvent(eventId,"FLIGHT_OPERATION_EVENT","1001");

        ProcessedEvent second = new ProcessedEvent(eventId,"FLIGHT_OPERATION_EVENT","1001");

        assertEquals(first.hashCode(), second.hashCode());
    }
}