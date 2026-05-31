package com.flightops.processing.exception;

public class PublishFailureEventException extends RuntimeException {
    public PublishFailureEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
