package com.flightops.processing.dto;

import com.flightops.contracts.enums.EventType;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents the JSON structure of an envelope that wraps a flight operation event.
 *
 * <p>An instance of this record serves as a container for key metadata and payload
 * information related to an event. It is primarily used for deserializing and processing
 * raw event messages, such as those consumed from Kafka topics.</p>
 *
 * <ul>
 *   <li>{@code eventId}: A UUID serving as a unique identifier for the event.</li>
 *   <li>{@code eventType}: The type of event, represented by the {@code EventType} enum.</li>
 *   <li>{@code aggregateId}: The identifier for the aggregate (e.g., a flight) that the event is associated with.</li>
 *   <li>{@code correlationId}: A correlation identifier for tracing and linking related events.</li>
 *   <li>{@code occurredAt}: The timestamp indicating when the event was generated.</li>
 *   <li>{@code payload}: The event payload, represented by {@link Payload}, containing details specific to the flight operation.</li>
 * </ul>
 */
public record EventEnvelopeJson(
        UUID eventId,
        EventType eventType,
        String aggregateId,
        String correlationId,
        Instant occurredAt,
        Payload payload
) {}