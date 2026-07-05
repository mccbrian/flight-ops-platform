package com.flightops.ingestion.exception;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.HandlerMapping;

import java.util.List;

/**
 * A centralized exception handler for handling various exceptions occurring in the ingestion service.
 * This class leverages Spring's {@code @RestControllerAdvice} to intercept and handle exceptions
 * across the application and returns custom error responses.
 * <p>
 * The handled exceptions include:
 * <ul>
 * - {@code MethodArgumentNotValidException}: Triggered when validation of a method argument fails.
 * - {@code HttpMessageNotReadableException}: Triggered when the request payload is malformed or improperly formatted.
 * - {@code EventPublishException}: Triggered when there is a failure in publishing an event.
 * - {@code Exception}: Catches any other unexpected runtime exceptions.
 * </ul>
 */
@RestControllerAdvice
public class IngestionExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidationException(
            MethodArgumentNotValidException exception,
            HttpServletRequest request
    ) {
        List<String> details = exception.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(this::formatFieldError)
                .toList();

        return ResponseEntity
                .badRequest()
                .body(ApiErrorResponse.of(
                        "REQUEST_VALIDATION_FAILED",
                        "Request validation failed",
                        correlationId(),
                        requestPath(request),
                        details
                ));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiErrorResponse> handleMalformedJson(HttpServletRequest request) {
        return ResponseEntity
                .badRequest()
                .body(ApiErrorResponse.of(
                        "INVALID_REQUEST_PAYLOAD",
                        "Request payload is malformed or contains unsupported values",
                        correlationId(),
                        requestPath(request)
                ));
    }

    @ExceptionHandler(EventPublishException.class)
    public ResponseEntity<ApiErrorResponse> handlePublishFailure(HttpServletRequest request) {
        return ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ApiErrorResponse.of(
                        "EVENT_PUBLISH_FAILED",
                        "Unable to accept flight operation event at this time",
                        correlationId(),
                        requestPath(request)
                ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleUnexpectedException(HttpServletRequest request) {
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiErrorResponse.of(
                        "INTERNAL_SERVER_ERROR",
                        "Unexpected ingestion service error",
                        correlationId(),
                        requestPath(request)
                ));
    }

    private String formatFieldError(FieldError error) {
        return error.getField() + ": " + error.getDefaultMessage();
    }

    private String correlationId() {
        return MDC.get("correlationId");
    }

    private String requestPath(HttpServletRequest request) {
        Object pattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);

        if (pattern instanceof String path && !path.isBlank()) {
            return path;
        }

        return "unknown";
    }

}