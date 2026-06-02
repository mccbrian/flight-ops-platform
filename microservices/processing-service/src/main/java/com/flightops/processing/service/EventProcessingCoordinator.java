package com.flightops.processing.service;

import com.flightops.contracts.failure.FailedEvent;
import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.producer.FailureEventProducer;
import com.flightops.processing.validation.ValidationError;
import com.flightops.processing.validation.ValidationErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessingCoordinator {

    private final ObjectMapper objectMapper;
    private final EventIdempotencyService idempotencyService;
    private final FlightOperationProcessingService processingService;
    private final FailureEventProducer failureEventProducer;

    @Value("${app.retry.max-attempts}")
    private int maxAttempts;

    /**
     * Processes a raw event message by parsing it, validating it, and delegating it
     * to downstream processing services.
     * <p>
     * If processing succeeds, the event is marked as processed in the idempotency store.
     * If processing fails, the event is routed to either the retry topic or dead-letter
     * topic depending on the retry ability of the failure and the current retry attempt count.
     *
     * @param rawMessage the raw event message received as a JSON string
     * @param attemptCount the current processing attempt count for the event,
     *                     starting at {@code 1} for initial ingestion and incremented
     *                     for each retry attempt
     */
    public void processRawEvent(String rawMessage, int attemptCount) {
        EventEnvelopeJson envelope = null;

        try {
            envelope = readEnvelope(rawMessage);

            if (!claimEvent(envelope)) {
                return;
            }

            FlightOperationEvent payload = toFlightOperationEvent(envelope);

            processingService.process(envelope, payload);

            idempotencyService.markProcessed(envelope.eventId());

            log.info(
                    "Event processed successfully. eventId={}, aggregateId={}",
                    envelope.eventId(),
                    envelope.aggregateId()
            );

        } catch (FlightOperationValidationException exception) {
            handleValidationFailure(envelope, rawMessage, attemptCount, exception);

        } catch (Exception exception) {
            handleUnexpectedFailure(envelope, rawMessage, attemptCount, exception);
        }
    }

    private EventEnvelopeJson readEnvelope(String rawMessage) {

        return objectMapper.readValue(rawMessage, EventEnvelopeJson.class);
    }

    private boolean claimEvent(EventEnvelopeJson envelope) {
        if (!idempotencyService.claimForProcessing(envelope.eventId())) {

            log.info(
                    "Event already claimed or processed. eventId={}, aggregateId={}",
                    envelope.eventId(),
                    envelope.aggregateId()
            );

            return false;
        }

        return true;
    }

    private FlightOperationEvent toFlightOperationEvent(
            EventEnvelopeJson envelope
    ) {
        return objectMapper.convertValue(
                envelope.payload(),
                FlightOperationEvent.class
        );
    }

    private void handleValidationFailure(
            EventEnvelopeJson envelope,
            String rawMessage,
            int attemptCount,
            FlightOperationValidationException exception
    ) {

        if (envelope == null) {
            log.error(
                    "Validation exception occurred before envelope was available. rawMessage={}",
                    rawMessage,
                    exception
            );
            return;
        }

        boolean retryable = isRetryable(exception);

        FailedEvent failedEvent = buildFailedEvent(
                envelope,
                rawMessage,
                retryable ? "RETRYABLE" : "NON_RETRYABLE",
                attemptCount,
                exception.errors().stream()
                        .map(ValidationError::code)
                        .toList(),
                exception
        );

        if (retryable && attemptCount < maxAttempts) {

            failureEventProducer.sendToRetry(failedEvent);

            log.warn(
                    "Validation failed with retryable errors. Routed to retry topic. eventId={}, attemptCount={}, errors={}",
                    exception.eventId(),
                    attemptCount,
                    exception.errors()
            );

            return;
        }

        failureEventProducer.sendToDlq(failedEvent);

        log.warn(
                "Validation failure routed to DLQ. eventId={}, attemptCount={}, retryable={}, errors={}",
                exception.eventId(),
                attemptCount,
                retryable,
                exception.errors()
        );
    }

    private void handleUnexpectedFailure(
            EventEnvelopeJson envelope,
            String rawMessage,
            int attemptCount,
            Exception exception
    ) {

        if (envelope == null) {
            log.error(
                    "Failed to parse raw event. rawMessage={}",
                    rawMessage,
                    exception
            );
            return;
        }

        FailedEvent failedEvent = buildFailedEvent(
                envelope,
                rawMessage,
                "RETRYABLE",
                attemptCount,
                List.of("UNEXPECTED_PROCESSING_ERROR"),
                exception
        );

        if (attemptCount < maxAttempts) {

            failureEventProducer.sendToRetry(failedEvent);

            log.error(
                    "Unexpected processing failure routed to retry topic. eventId={}, attemptCount={}, rawMessage={}",
                    envelope.eventId(),
                    attemptCount,
                    rawMessage,
                    exception
            );

            return;
        }

        failureEventProducer.sendToDlq(failedEvent);

        log.error(
                "Unexpected processing failure routed to DLQ. eventId={}, attemptCount={}, rawMessage={}",
                envelope.eventId(),
                attemptCount,
                rawMessage,
                exception
        );
    }

    private FailedEvent buildFailedEvent(
            EventEnvelopeJson envelope,
            String rawMessage,
            String classification,
            int attemptCount,
            List<String> errors,
            Exception exception
    ) {

        idempotencyService.releaseClaim(envelope.eventId());

        return new FailedEvent(
                envelope.eventId(),
                envelope.eventType().name(),
                envelope.aggregateId(),
                envelope.correlationId(),
                classification,
                errors,
                exception.getMessage(),
                safeMessageMap(rawMessage),
                attemptCount,
                maxAttempts,
                Instant.now()
        );
    }

    private Map<String, Object> safeMessageMap(String rawMessage) {

        try {
            return objectMapper.readValue(rawMessage, new TypeReference<>() {});

        } catch (Exception exception) {

            log.error(
                    "Failed to deserialize raw message into map.",
                    exception
            );

            return Map.of("rawMessage", rawMessage);
        }
    }

    private boolean isRetryable(
            FlightOperationValidationException exception
    ) {

        return exception.errors()
                .stream()
                .anyMatch(error ->
                        error.type() == ValidationErrorType.RETRYABLE
                );
    }

}