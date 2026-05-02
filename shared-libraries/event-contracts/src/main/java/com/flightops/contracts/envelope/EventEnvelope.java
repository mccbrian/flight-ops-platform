package com.flightops.contracts.envelope;

import com.flightops.contracts.enums.EventType;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(
        UUID eventId,
        EventType eventType,
        String aggregateId,
        Instant processedAt,
        T payload
) implements Serializable {}

