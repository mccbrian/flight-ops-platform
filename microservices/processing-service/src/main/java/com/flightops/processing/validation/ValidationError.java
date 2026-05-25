package com.flightops.processing.validation;

/**
 * Represents a validation error encountered during a validation process.
 *
 * <ul>
 *     <li>{@code type} indicates whether the error is retryable or non-retryable,
 *         based on the {@link ValidationErrorType} enum.</li>
 *     <li>{@code code} provides a unique identifier for the specific validation rule or condition
 *         that was violated.</li>
 *     <li>{@code message} offers a descriptive explanation of the validation failure.</li>
 * </ul>
 *
 * Instances of this record are typically used to capture and communicate details
 * about individual validation failures as part of a {@link ValidationResult}.
 */
public record ValidationError(
        ValidationErrorType type,
        String code,
        String message
) {}