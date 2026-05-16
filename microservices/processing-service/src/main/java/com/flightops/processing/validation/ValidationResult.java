package com.flightops.processing.validation;

import java.util.List;
import java.util.Objects;

public record ValidationResult(
        boolean valid,
        List<ValidationError> errors
) {

    public ValidationResult {
        Objects.requireNonNull(errors, "errors must not be null");

        // Make defensive immutable copy
        errors = List.copyOf(errors);

        // Enforce invariants
        if (valid && !errors.isEmpty()) {
            throw new IllegalArgumentException("Valid result cannot contain errors");
        }

        if (!valid && errors.isEmpty()) {
            throw new IllegalArgumentException("Invalid result must contain at least one error");
        }
    }

    public static ValidationResult success() {
        return new ValidationResult(true, List.of());
    }

    public static ValidationResult failure(List<ValidationError> errors) {
        return new ValidationResult(false, errors);
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public boolean hasRetryableErrors() {
        return errors.stream()
                .anyMatch(validationError -> validationError.type() == ValidationErrorType.RETRYABLE);
    }

    public boolean hasOnlyNonRetryableErrors() {
        return !valid
                && !errors.isEmpty()
                && errors.stream().allMatch(validationError -> validationError.type() == ValidationErrorType.NON_RETRYABLE);
    }

}