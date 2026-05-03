package com.flightops.contracts.envelope;

import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.ingestion.FlightOperationEvent;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        EventType eventType,
        String aggregateId,
        String correlationId,
        Instant occurredAt,
        FlightOperationEvent payload
) {}

