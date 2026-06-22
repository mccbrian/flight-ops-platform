package com.flightops.processing.validation;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.repository.FlightReferenceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates {@link FlightOperationEvent} instances before they are processed
 * by the flight operations pipeline.
 * <p>
 * This validator ensures that incoming events are both structurally valid and
 * consistent with system expectations before being applied to the flight state model.
 * <p>
 * Validation covers:
 * <ul>
 *     <li>Referential integrity (flight must exist)</li>
 *     <li>Validity of an operation type</li>
 *     <li>Field-level constraints based on event payload</li>
 *     <li>Temporal validity of the event timestamp (bounded clock skew validation)</li>
 * </ul>
 * The validator does not modify the event or apply business state transitions.
 * It only enforces preconditions required for safe downstream processing.
 * Results are returned as a {@link ValidationResult} containing any detected violations.
 */
@Component
@RequiredArgsConstructor
public class FlightOperationValidator {

    private final FlightReferenceRepository flightReferenceRepository;

    /**
     * Validates a {@link FlightOperationEvent} against structural, referential,
     * and temporal rules.
     * <p>
     * This validation ensures the event can safely be applied to the flight
     * operation state model without violating system constraints or ordering rules.
     *
     * @param event the flight operation event to validate
     * @return a {@link ValidationResult} indicating whether the event is valid,
     *         or containing validation errors if constraints are violated
     */
    public ValidationResult validate(FlightOperationEvent event) {
        List<ValidationError> errors = new ArrayList<>();

        validateFlightExists(event, errors);
        validateOperationType(event, errors);
        validateStatus(event, errors);
        validateGate(event, errors);
        validateDelayMinutes(event, errors);
        validateReason(event, errors);
        validateEventTime(event, errors);

        return errors.isEmpty()
                ? ValidationResult.success()
                : ValidationResult.failure(errors);
    }

    private void validateFlightExists(FlightOperationEvent event, List<ValidationError> errors) {
        if (!flightReferenceRepository.existsByFlightId(event.getFlightId())) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "FLIGHT_NOT_FOUND",
                    "No flight exists for flight ID=" + event.getFlightId()
            ));
        }
    }

    private void validateOperationType(FlightOperationEvent event, List<ValidationError> errors) {
        try {
            OperationType.valueOf(event.getOperationType());
        } catch (IllegalArgumentException exception) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "UNKNOWN_OPERATION_TYPE",
                    "Unsupported operation type=" + event.getOperationType()
            ));
        }
    }

    private void validateStatus(FlightOperationEvent event, List<ValidationError> errors) {
        if (event.getStatus().isBlank()) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "STATUS_REQUIRED",
                    "status must not be blank"
            ));
        }
    }

    private void validateGate(FlightOperationEvent event, List<ValidationError> errors) {
        if (event.getGate().isBlank()) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "GATE_REQUIRED",
                    "gate must not be blank"
            ));
        }
    }

    private void validateDelayMinutes(FlightOperationEvent event, List<ValidationError> errors) {
        if (event.getDelayMinutes() < 0) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "INVALID_DELAY_MINUTES",
                    "delay minutes must not be negative"
            ));
        }
    }

    private void validateReason(FlightOperationEvent event, List<ValidationError> errors) {
        if (event.getReason().isBlank()) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "REASON_REQUIRED",
                    "reason must not be blank"
            ));
        }
    }

    public void validateEventTime(FlightOperationEvent event, List<ValidationError> errors) {
        Instant now = Instant.now();

        if (event.getEventTime().isAfter(now.plusSeconds(10))) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "EVENT_TIME_CLOCK_SKEW_TOO_LARGE",
                    "event time is outside the allowed clock skew window (too far in the future)"
            ));
        }
    }

}