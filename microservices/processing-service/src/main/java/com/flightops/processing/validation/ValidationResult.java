package com.flightops.processing.validation;

import java.util.List;
import java.util.Objects;

/**
 * Represents the outcome of a validation process, encapsulating its validity
 * and any associated validation errors.
 *
 * <ul>
 *     <li>{@code valid} indicates whether the validation was successful.</li>
 *     <li>{@code errors} contains a list of {@link ValidationError} instances describing
 *         the validation failures, if any.</li>
 * </ul>
 *
 * This class enforces the following invariants:
 * <ul>
 *     <li>A valid result cannot contain any errors.</li>
 *     <li>An invalid result must contain at least one error.</li>
 * </ul>
 *
 * It provides factory methods for creating successful and failure results, along with
 * utility methods to inspect the nature of the validation errors.
 */
public record ValidationResult(
        boolean valid,
        List<ValidationError> errors
) {

    public ValidationResult {
        Objects.requireNonNull(errors, "errors must not be null");

        errors = List.copyOf(errors);

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