package com.flightops.contracts.failure;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * Represents a failed event that occurs during processing or validation.
 * <p>
 * Instances of this record are used to encapsulate details about an event that could not
 * be successfully processed, storing metadata and diagnostic information to aid in
 * troubleshooting and retry mechanisms.
 *
 * @param originalEventId the identifier of the original event that failed
 * @param originalEventType the type of the original event
 * @param aggregateId the aggregate identifier associated with the event
 * @param correlationId the correlation identifier used for distributed tracing
 * @param failureType the classification of the failure
 *                    (for example, {@code RETRYABLE} or {@code NON_RETRYABLE})
 * @param errorCodes the validation or processing error codes associated with the failure
 * @param reason a human-readable description of the failure
 * @param payloadMap the structured map representing the deserialized event payload received for processing
 * @param failedAt the timestamp indicating when the failure occurred
 */
public record FailedEvent(
        UUID originalEventId,
        String originalEventType,
        String aggregateId,
        String correlationId,
        String failureType,
        List<String> errorCodes,
        String reason,
        Map<String, Object> payloadMap,
        Instant failedAt
) {}