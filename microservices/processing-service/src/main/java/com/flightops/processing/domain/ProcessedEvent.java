package com.flightops.processing.domain;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Persistent record of successfully processed flight operation events.
 * <p>
 * This entity functions as an idempotency and audit log, ensuring that each event is only processed once within the system.
 * <p>
 * Each record represents a single processed event and is immutable after creation. It is stored in an append-only manner
 * and is never updated.
 * <h2>
 * idempotency
 * <p>
 * This entity is used to enforce idempotent processing by allowing the system to detect whether an event with a given
 * {@code eventId} has already been successfully processed.
 * <h2>
 * Persistence behavior
 * <p>
 * This entity is always treated as new and is always inserted into the underlying datastore. Updates are not supported
 * or expected.
 * <p>
 * The {@code processedAt} timestamp reflects the time at which the event was successfully processed by the system.
 * <h2>
 * Identity
 * <p>
 * Equality and identity are based solely on {@code eventId}, which uniquely
 * identifies the processed event.
 */
@Getter
@Setter
@Table("processed_events")
public class ProcessedEvent implements Persistable<UUID> {

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

    @Override
    public UUID getId() {
        return eventId;
    }

    @Override
    public boolean isNew() {
        return true; // always insert for event log style entities
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessedEvent that = (ProcessedEvent) o;

        return eventId != null && eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventId);
    }

}
