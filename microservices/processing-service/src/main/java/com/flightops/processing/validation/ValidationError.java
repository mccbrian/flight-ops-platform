package com.flightops.processing.validation;

public record ValidationError(
        ValidationErrorType type,
        String code,
        String message
) {}