package com.flightops.processing.exception;

public class EventParsingException extends RuntimeException {
    public EventParsingException(String message, Throwable cause) {
        super(message, cause);
    }
}
