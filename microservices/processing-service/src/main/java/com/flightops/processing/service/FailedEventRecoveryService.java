package com.flightops.processing.service;

import com.flightops.contracts.avro.FailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service responsible for recovering failed events by re-submitting them for processing with an incremented retry
 * attempt count.
 * <p>
 * This service acts as a thin orchestration layer that takes a previously failed event and delegates it back into
 * the standard processing pipeline via {@link EventProcessingCoordinator}.
 * <p>
 * The recovery process:
 * <ul>
 *   <li>Calculates the next retry attempt count based on the failed event metadata</li>
 *   <li>Logs recovery initiation details for observability</li>
 *   <li>Re-submits the original raw payload for reprocessing</li>
 * </ul>
 * No deserialization, mapping, or business logic is performed in this service; all processing is delegated to the
 * standard event processing workflow.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FailedEventRecoveryService {

    private final EventProcessingCoordinator coordinator;

    /**
     * Attempts to recover a failed event by reprocessing its raw payload with an increment in the retry attempt count.
     * The recovery process logs the details of the failed event and delegates the raw payload to the
     * event processing coordinator.
     * <p>
     * The recovery process includes:
     * <ul>
     *   <li>Parsing the raw JSON string into a {@link FailedEvent} object.</li>
     *   <li>Logging the original event ID, attempt count, failure type, and associated error codes.</li>
     *   <li>Extracting the original event payload.</li>
     *   <li>Incrementing the retry attempt count from the failed event metadata.</li>
     *   <li>Passing the payload and updated retry attempt count to the {@code EventProcessingCoordinator} for reprocessing.</li>
     * </ul>
     *
     * @param failedEvent the failed event to be recovered, containing details such as the original payload, aggregate ID,
     *                    and the number of prior processing attempts
     */
    public void recover(FailedEvent failedEvent) {
        int nextAttempt = failedEvent.getAttemptCount() + 1;

        log.info("Recovering failed event. originalEventId={}, aggregateId={}, attemptCount={}, nextAttempt={}",
                failedEvent.getOriginalEventId(),
                failedEvent.getAggregateId(),
                failedEvent.getAttemptCount(),
                nextAttempt);

        coordinator.processRawEvent(failedEvent.getRawPayload(), nextAttempt);
    }

}