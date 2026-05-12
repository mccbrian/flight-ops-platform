package com.flightops.processing.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("processed_events")
public class ProcessedEvent {

    @Id
    private UUID eventId;

    private String eventType;
    private String aggregateId;
    private Instant processedAt;

    public ProcessedEvent(UUID eventId, String eventType, String aggregateId) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.processedAt = Instant.now();
    }

    public UUID getEventId() { return eventId; }

}
