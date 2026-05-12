package com.flightops.processing.dto;

import com.flightops.contracts.enums.EventType;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelopeJson(
        UUID eventId,
        EventType eventType,
        String aggregateId,
        String correlationId,
        Instant occurredAt,
        //using Object allows Jackson to parse it into a standard Java Map
        Object payload
) {
}