package com.flightops.contracts.envelope;

import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.ingestion.FlightOperationEvent;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents an immutable envelope for transferring events with metadata.
 * This generic record is designed to encapsulate a payload along with
 * relevant metadata, such as event type, identifiers, and timestamps.
 *
 * @param <T>          The type of the payload encapsulated in the envelope.
 * @param eventId      A unique identifier for the event, typically a UUID.
 * @param eventType    The type of the event, as defined by the {@code EventType} enum.
 * @param aggregateId  An identifier representing the aggregate object
 *                     associated with the event, often used for routing
 *                     and handling in distributed systems.
 * @param correlationId A unique identifier for correlating related events.
 *                      This is useful for tracking and debugging event flows.
 * @param occurredAt   The timestamp indicating when the event occurred.
 * @param payload      The actual content or message associated with the event.
 */
public record EventEnvelope<T>(
        UUID eventId,
        EventType eventType,
        String aggregateId,
        String correlationId,
        Instant occurredAt,
        T payload
) {}

