package com.flightops.processing.component;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.validation.ValidationError;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;

/**
 * Factory responsible for constructing standardized {@link FailedEvent} payloads used for retry and dead-letter processing.
 * <p>
 * This component encapsulates the creation of failure events, including:
 * <ul>
 *   <li>Extracting metadata from the original event envelope</li>
 *   <li>Serializing and normalizing the event payload for downstream systems</li>
 *   <li>Attaching classification and error information</li>
 *   <li>Including retry metadata such as attempt count and max attempts</li>
 * </ul>
 * It also ensures the event claim is released in the idempotency store so that failed events can be retried if applicable.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FailedEventFactory {

    private final ObjectMapper objectMapper;

    @Value("${app.retry.max-attempts}")
    private int maxAttempts;

    /**
     * Builds a standardized {@link FailedEvent} from an event envelope and associated failure context.
     * <p>
     * The method releases any existing processing claim for the event in the idempotency store before constructing the
     * failure payload.
     * <p>
     * The original event envelope is serialized and converted into a structured map representation to support downstream
     * processing and debugging.
     *
     * @param envelope the original event envelope that failed to process
     * @param classification failure classification (e.g., RETRYABLE or NON_RETRYABLE)
     * @param errors list of validation or processing error codes associated with the failure
     * @param reason a human-readable description of the failure
     * @param attemptCount the current processing attempt count
     * @return a fully constructed {@link FailedEvent} ready for routing
     */
    public FailedEvent buildFailedEvent(
            EventEnvelopeJson envelope,
            boolean classification,
            List<ValidationError> errors,
            String reason,
            int attemptCount
    ) {
        return FailedEvent.newBuilder()
                .setOriginalEventId(envelope.eventId().toString())
                .setOriginalEventType(envelope.eventType().name())
                .setAggregateId(envelope.aggregateId())
                .setCorrelationId(envelope.correlationId())
                .setFailureType(classification ? "RETRYABLE" : "NON_RETRYABLE")
                .setErrorCodes(errors.stream().map(ValidationError::code).toList())
                .setReason(reason)
                .setRawPayload(toRawEnvelopeJson(envelope))
                .setAttemptCount(attemptCount)
                .setMaxAttempts(maxAttempts)
                .setFailedAt(Instant.now())
                .build();
    }

    private String toRawEnvelopeJson(EventEnvelopeJson envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to serialize original event envelope", exception);
        }
    }

}