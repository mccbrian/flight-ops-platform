package com.flightops.processing.domain;

import com.flightops.contracts.avro.FlightOperationEvent;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.Objects;

/**
 * Persistent projection representing the current operational state of a flight.
 * <p>
 * This entity acts as a materialized view of flight operations, maintained by applying
 * incoming {@link FlightOperationEvent} instances in event-time order.
 * It stores only the latest known state of a flight and is not an event log.
 * <p>
 * Each instance is uniquely identified by {@code flightId}, which serves as both the
 * database primary key and the domain identity of the flight.
 *
 * <h2>State management</h2>
 * This entity is updated in-place using {@link #applySnapshot(FlightOperationEvent)}.
 * Each incoming event replaces the previous state if it is newer than the last applied
 * event.
 * <p>
 * The {@code lastEventTime} field is used to ensure events are applied in order and to
 * prevent stale updates from overwriting newer state.
 * <p>
 * Every update also refreshes {@code updatedAt}, which represents the last time the
 * projection was modified.
 *
 * <h2>Concurrency</h2>
 * The {@code version} field is used by Spring Data JDBC for optimistic locking.
 * Concurrent modifications to the same flight status will result in an optimistic
 * locking failure rather than silently overwriting changes.
 *
 * <h2>Equality</h2>
 * Equality and hash code are based solely on {@code flightId}, which is the stable
 * business identifier of the flight.
 */
@Getter
@Setter
@NoArgsConstructor
@Table("flight_operation_status")
public class FlightOperationStatus {

    @Id
    private Integer flightId;

    @Version
    private Long version;

    private String operationType;
    private String status;
    private String gate;
    private Integer delayMinutes;
    private String reason;
    private Instant lastEventTime;
    private Instant updatedAt;

    public static FlightOperationStatus initialize(Integer flightId) {
        FlightOperationStatus status = new FlightOperationStatus();
        status.flightId = flightId;
        status.updatedAt = Instant.now();
        return status;
    }

    public boolean hasNewerOrSameEventThan(Instant eventTime) {
        return this.lastEventTime != null
                && eventTime != null
                && !this.lastEventTime.isBefore(eventTime);
    }

    public void applySnapshot(FlightOperationEvent event) {
        this.operationType = event.getOperationType();
        this.status = event.getStatus();
        this.gate = event.getGate();
        this.delayMinutes = event.getDelayMinutes();
        this.reason = event.getReason();
        this.lastEventTime = event.getEventTime();
        this.updatedAt = Instant.now();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlightOperationStatus that = (FlightOperationStatus) o;
        return flightId != null && flightId.equals(that.flightId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(flightId);
    }

}
