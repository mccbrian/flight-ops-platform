package com.flightops.processing.component;

import com.flightops.contracts.failure.FailedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.metrics.FlightOperationMetrics;
import com.flightops.processing.producer.FailureEventProducer;
import com.flightops.processing.validation.ValidationError;
import com.flightops.processing.validation.ValidationErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Responsible for classifying and routing failed flight operation events to either retry or dead-letter processing channels.
 * <p>
 * This component centralizes failure handling logic that includes:
 * <ul>
 *   <li>Classifying failures as retryable or non-retryable</li>
 *   <li>Constructing standardized {@link FailedEvent} payloads</li>
 *   <li>Routing events to retry or DLQ topics based on failure type and attempt count</li>
 *   <li>Recording failure, retry, and DLQ metrics</li>
 * </ul>
 * Retry decisions are based on both the failure classification and the configured maximum attempt threshold.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FailureRouter {

    private final FlightOperationMetrics metrics;
    private final FailedEventFactory failedEventFactory;
    private final FailureEventProducer failureEventProducer;

    @Value("${app.retry.max-attempts}")
    private int maxAttempts;

    /**
     * Handles validation failures by classifying the exception, building a standardized {@link FailedEvent}, and routing
     * it to either retry or DLQ based on retry eligibility and attempt count.
     * <p>
     * If the envelope is not available, the failure is logged and counted as a terminal failure without further processing.
     *
     * @param envelope the event envelope associated with the failure, or null if parsing failed before envelope creation
     * @param attemptCount the current processing attempt count for the event
     * @param exception the validation exception containing error details and validation error codes
     */
    public void routeValidationFailure(EventEnvelopeJson envelope, int attemptCount, FlightOperationValidationException exception) {

        if (envelope == null) {
            log.error("Validation failure occurred before envelope was available.", exception);
            metrics.incrementFailed();
            return;
        }

        boolean retryable = isRetryable(exception);
        String classification = retryable ? "RETRYABLE" : "NON_RETRYABLE";

        FailedEvent failedEvent = failedEventFactory.buildFailedEvent(
                envelope,
                classification,
                attemptCount,
                exception.errors().stream()
                        .map(ValidationError::code)
                        .toList(),
                exception
        );

        routeFailedEvent(failedEvent, retryable, attemptCount, "Validation failure");
    }

    /**
     * Handles unexpected processing failures by treating them as retryable system-level errors and routing them through
     * the standard failure routing workflow.
     * <p>
     * A standardized {@link FailedEvent} is created with a generic error code and routed to retry or DLQ depending on
     * attempt count.
     * <p>
     * If the envelope is not available, the failure is logged and counted as a terminal failure without further processing.
     *
     * @param envelope the event envelope associated with the failure, or null if parsing failed before envelope creation
     * @param attemptCount the current processing attempt count for the event
     * @param exception the unexpected exception that occurred during processing
     */
    public void handleUnexpectedFailure(EventEnvelopeJson envelope, int attemptCount, Exception exception) {

        if (envelope == null) {
            log.error("Unexpected failure occurred before envelope was available.", exception);
            metrics.incrementFailed();
            return;
        }

        FailedEvent failedEvent = failedEventFactory.buildFailedEvent(
                envelope,
                "RETRYABLE",
                attemptCount,
                List.of("UNEXPECTED_PROCESSING_ERROR"),
                exception
        );

        routeFailedEvent(failedEvent, true, attemptCount, "Unexpected processing failure");
    }

    private void routeFailedEvent(FailedEvent failedEvent, boolean retryable, int attemptCount, String failureContext) {

        if (retryable && attemptCount < maxAttempts) {
            failureEventProducer.sendToRetry(failedEvent);
            metrics.incrementRetry();

            log.warn(
                    "{} routed to retry topic. eventId={}, attemptCount={}, errorCodes={}",
                    failureContext,
                    failedEvent.originalEventId(),
                    attemptCount,
                    failedEvent.errorCodes()
            );

            return;
        }

        failureEventProducer.sendToDlq(failedEvent);
        metrics.incrementDlq();
        metrics.incrementFailed();

        log.warn(
                "{} routed to DLQ. eventId={}, attemptCount={}, retryable={}, errorCodes={}",
                failureContext,
                failedEvent.originalEventId(),
                attemptCount,
                retryable,
                failedEvent.errorCodes()
        );
    }

    private boolean isRetryable(FlightOperationValidationException exception) {
        return exception.errors()
                .stream()
                .anyMatch(error -> error.type() == ValidationErrorType.RETRYABLE);
    }

}