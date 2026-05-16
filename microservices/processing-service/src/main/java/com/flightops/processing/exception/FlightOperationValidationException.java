package com.flightops.processing.exception;

import com.flightops.processing.validation.ValidationError;

import java.util.List;
import java.util.UUID;

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