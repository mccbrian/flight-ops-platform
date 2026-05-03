package com.flightops.processing.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelopeJson(
        UUID eventId,
        String eventType,
        String aggregateId,
        String correlationId,
        Instant occurredAt,
        Object payload
) {
}
