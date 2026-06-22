package com.flightops.processing.component;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.contracts.enums.OperationType;
import org.springframework.stereotype.Component;

/**
 * Component responsible for applying {@link FlightOperationEvent} updates
 * to a {@link FlightOperationStatus} projection.
 * <p>
 * This updater is responsible for maintaining a consistent flight state by
 * applying events in event-time order. It ensures that stale or out-of-order
 * events do not overwrite newer state.
 * <p>
 * Each {@link OperationType} is carried within
 * the event payload, but this component does not branch on operation type.
 * Instead, it applies a full snapshot of the event to the current state.
 * <p>
 * The updater is stateless and does not perform persistence operations.
 * It only applies in-memory state transitions and returns whether the update
 * was accepted.
 */
@Component
public class FlightOperationStatusUpdater {

    /**
     * Applies a {@link FlightOperationEvent} to the given {@link FlightOperationStatus}
     * if the event is newer than the currently stored state.
     * <p>
     * Event ordering is enforced using {@code eventTime}. If the incoming event
     * is older than or equal to the latest applied event, it is ignored and the
     * method returns {@code false}.
     * <p>
     * When accepted, the event is applied as a full snapshot, updating all relevant
     * fields on the target status object.
     *
     * @param status the current flight operation state projection
     * @param event the incoming flight operation event
     * @return {@code true} if the event was applied, {@code false} if it was ignored
     *         due to being older than the current state
     */
    public boolean apply(FlightOperationStatus status, FlightOperationEvent event) {
        if (status.hasNewerOrSameEventThan(event.getEventTime())) {
            return false;
        }

        status.applySnapshot(event);
        return true;
    }

}