package com.flightops.processing.exception;

import com.flightops.processing.validation.ValidationError;

import java.util.List;
import java.util.UUID;

/**
 * Exception thrown when validation of a flight operation fails.
 *
 * <p>This exception is typically used to encapsulate validation errors that occur
 * during the processing of flight operation events. It contains an event identifier
 * and a list of detailed validation errors to assist in diagnosing and handling
 * the validation failure.</p>
 *
 * <p>Instances of this exception are often leveraged in scenarios where validation
 * logic determines whether an event can proceed for processing, or needs to be rerouted
 * to a retry or dead-letter mechanism based on the nature of the errors.</p>
 *
 * <p>Constructors:
 * <ul>
 *     <li>{@code eventId} represents the unique identifier of the event related
 *     to the validation failure.</li>
 *     <li>{@code errors} is a list of {@link ValidationError} objects describing
 *     the specific rules or conditions that failed validation.</li>
 * </ul>
 *
 * <p>Key methods include:
 * <ul>
 *     <li>{@code eventId()} for retrieving the identifier of the failed event.</li>
 *     <li>{@code errors()} for accessing the list of validation error details.</li>
 * </ul>
 *
 * <p>This class extends {@code RuntimeException}, allowing it to be used for
 * unchecked exception handling scenarios where validation failures need to
 * propagate through the call stack.</p>
 */
public class FlightOperationValidationException extends RuntimeException {

    private final UUID eventId;
    private final List<ValidationError> errors;

    public FlightOperationValidationException(UUID eventId, List<ValidationError> errors) {
        super("Flight operation validation failed for eventId=" + eventId);
        this.eventId = eventId;
        this.errors = errors;
    }

    public UUID eventId() {
        return eventId;
    }

    public List<ValidationError> errors() {
        return errors;
    }

}