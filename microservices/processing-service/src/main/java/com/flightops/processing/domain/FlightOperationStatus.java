package com.flightops.processing.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.Objects;

/**
 * Persistent aggregate representing the current operational state of a flight.
 * <p>
 * This entity stores the latest known state of a flight as maintained by the flight operation processing system. It
 * is updated in-place whenever a new flight operation event is processed.
 * <p>
 * Each instance is uniquely identified by {@code flightId}, which serves as both the database primary key and the domain
 * identity of the flight.
 * <h2>
 * State management
 * <p>The entity is mutable by design. Updates are applied via the
 * {@link #update(String, String, Integer)} method, which modifies the existing
 * record rather than replacing it.
 * <p>
 * Each update also refreshes {@code updatedAt} to reflect the last time the
 * flight state was modified.
 * <h2>
 * Persistence behavior
 * <p>This class implements {@link org.springframework.data.domain.Persistable}
 * to explicitly control insert vs. update behavior in Spring Data.
 * <p>
 * The {@code isNew} flag determines whether the entity should be inserted or
 * updated:
 * <ul>
 *   <li>{@code true} → entity is treated as new and will be inserted</li>
 *   <li>{@code false} → entity is treated as existing and will be updated</li>
 * </ul>
 * New instances created via the all-args constructor are marked as new.
 * After calling {@link #update(String, String, Integer)}, the entity is
 * considered existing.</p>
 * <h2>
 * Equality
 * <p>
 * Equality and hash code are based solely on {@code flightId}, as this is
 * the stable business identity of the flight.
 */
@Getter
@Setter
@NoArgsConstructor
@Table("flight_operation_status")
public class FlightOperationStatus implements Persistable<Integer> {

    @Id
    private Integer flightId;

    private String status;
    private String gate;
    private Integer delayMinutes;
    private Instant updatedAt;

    @Transient
    private boolean isNew;

    public FlightOperationStatus(Integer flightId, String status, String gate, Integer delayMinutes) {
        this.flightId = flightId;
        this.status = status;
        this.gate = gate;
        this.delayMinutes = delayMinutes;
        this.updatedAt = Instant.now();
        this.isNew = true;
    }

    public void update(String status, String gate, Integer delayMinutes) {
        this.status = status;
        this.gate = gate;
        this.delayMinutes = delayMinutes;
        this.updatedAt = Instant.now();
        this.isNew = false;
    }

    @Override
    public Integer getId() {
        return flightId;
    }

    @Override
    public boolean isNew() {
        return isNew;
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
