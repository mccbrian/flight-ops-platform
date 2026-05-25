package com.flightops.processing.validation;

import com.flightops.contracts.enums.OperationType;
import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.repository.FlightReferenceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates {@link FlightOperationEvent} payloads against required field,
 * reference integrity, and operation-specific business rules.
 *
 * <p>The validator checks for the presence of mandatory attributes,
 * verifies that referenced flights exist, and enforces conditional
 * validation rules based on the {@link OperationType}.</p>
 *
 * <p>Validation failures are collected and returned as a
 * {@link ValidationResult} containing one or more {@link ValidationError}
 * instances.</p>
 */
@Component
@RequiredArgsConstructor
public class FlightOperationValidator {

    private final FlightReferenceRepository flightReferenceRepository;

    /**
     * Validates the supplied flight operation event payload.
     *
     * @param payload the flight operation event to validate
     * @return a successful {@link ValidationResult} if validation passes;
     *         otherwise a failure result containing one or more validation errors
     */
    public ValidationResult validate(FlightOperationEvent payload) {
        List<ValidationError> errors = new ArrayList<>();

        if (payload.flightId() == null) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "FLIGHT_ID_REQUIRED",
                    "flight ID is required"
            ));
        }

        if (payload.operationType() == null) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "OPERATION_TYPE_REQUIRED",
                    "operation type is required"
            ));
        }

        if (payload.flightId() != null && !flightReferenceRepository.existsByFlightId(payload.flightId())) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "FLIGHT_NOT_FOUND",
                    "No flight exists for flight ID=" + payload.flightId()
            ));
        }

        if (payload.operationType() == OperationType.DELAY && payload.delayMinutes() == null) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "DELAY_MINUTES_REQUIRED",
                    "delay minutes is required for DELAY operations"
            ));
        }

        if (payload.operationType() == OperationType.GATE_CHANGE && payload.gate() == null) {
            errors.add(new ValidationError(
                    ValidationErrorType.NON_RETRYABLE,
                    "GATE_REQUIRED",
                    "gate is required for GATE_CHANGE operations"
            ));
        }

        return errors.isEmpty()
                ? ValidationResult.success()
                : ValidationResult.failure(errors);
    }

}