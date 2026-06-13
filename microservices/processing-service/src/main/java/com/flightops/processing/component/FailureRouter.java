package com.flightops.processing.component;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.metrics.FlightOperationMetrics;
import com.flightops.processing.producer.FailureEventProducer;
import com.flightops.processing.validation.ValidationErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.UUID;

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
    private final EventIdempotencyService idempotencyService;

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

        boolean classification = isRetryable(exception);

        FailedEvent failedEvent = failedEventFactory.buildFailedEvent(
                envelope,
                classification,
                exception.errors(),
                exception.getMessage(),
                attemptCount
        );

        routeFailedEvent(failedEvent, classification, attemptCount, "Validation failure");
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
                true,
                Collections.emptyList(),
                exception.getMessage(),
                attemptCount
        );

        routeFailedEvent(failedEvent, true, attemptCount, "Unexpected processing failure");
    }

    /**
     * Routes a failed event to either the retry topic or the dead-letter queue (DLQ) based on the failure classification
     * <p>
     * <b>IMPORTANT:</b> Always release the idempotency claim before routing to retry or DLQ. If the claim is not released,
     * Redis will block the retry consumer from reprocessing the event, causing the event to remain stuck in PROCESSING
     * state indefinitely.
     * <p>
     * This release is required even for DLQ events because no further processing will occur.
     * @param failedEvent the enriched failed event to be routed
     * @param retryable whether the failure is eligible for retry
     * @param attemptCount the current processing attempt count
     * @param failureContext a human-readable description of the failure scenario for logging
     */
    private void routeFailedEvent(FailedEvent failedEvent, boolean retryable, int attemptCount, String failureContext) {

        UUID originalEventId = UUID.fromString(failedEvent.getOriginalEventId());
        idempotencyService.releaseClaim(originalEventId);

        if (retryable && attemptCount < maxAttempts) {
            failureEventProducer.sendToRetry(failedEvent);
            metrics.incrementRetry();

            log.warn(
                    "{} routed to retry topic. eventId={}, attemptCount={}, errorCodes={}",
                    failureContext,
                    failedEvent.getOriginalEventId(),
                    attemptCount,
                    failedEvent.getErrorCodes()
            );

            return;
        }
        failureEventProducer.sendToDlq(failedEvent);
        metrics.incrementDlq();
        metrics.incrementFailed();

        log.warn(
                "{} routed to DLQ. eventId={}, attemptCount={}, retryable={}, errorCodes={}",
                failureContext,
                failedEvent.getOriginalEventId(),
                attemptCount,
                retryable,
                failedEvent.getErrorCodes()
        );
    }

    private boolean isRetryable(FlightOperationValidationException exception) {
        return exception.errors()
                .stream()
                .anyMatch(error -> error.type() == ValidationErrorType.RETRYABLE);
    }

}