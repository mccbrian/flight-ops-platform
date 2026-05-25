package com.flightops.processing.validation;

/**
 * Specifies the type of validation error, indicating whether the error
 * is retryable or non-retryable.
 *
 * <ul>
 *     <li>{@code NON_RETRYABLE} indicates a validation error that cannot
 *         be retried and requires manual intervention or corrective
 *         measures to resolve.</li>
 *     <li>{@code RETRYABLE} indicates a validation error that can potentially
 *         be resolved through subsequent attempts or corrective actions
 *         without requiring manual intervention.</li>
 * </ul>
 *
 * This enum is used in conjunction with {@link ValidationError} to classify
 * validation failures in a validation process.
 */
public enum ValidationErrorType {
    NON_RETRYABLE,
    RETRYABLE
}