package com.flightops.contracts.envelope;

import java.io.Serializable;
import java.time.Instant;

public record EventEnvelope<T>(
        String eventId,
        String eventType,
        String aggregateId,
        Instant occurredAt,
        T payload
) implements Serializable {}

