package com.flightops.processing.service;

import com.flightops.contracts.failure.FailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Service to handle the recovery of failed events. This service processes events
 * that previously failed to process and attempts recovery by re-submitting the
 * original payload for coordinated reprocessing with retry attempt tracking.
 * <p>
 * The recovery process involves:
 * <ol>
 *   <li>Converting a raw failed event JSON string into a {@code FailedEvent} object.</li>
 *   <li>Logging relevant details about the failed event, such as its original ID, failure type, and error codes.</li>
 *   <li>Extracting the original payload and calculating the next retry attempt count.</li>
 *   <li>Passing the payload and updated retry attempt count to the {@code EventProcessingCoordinator} for reprocessing.</li>
 *   <li>Handling and logging any exceptions that occur during the recovery attempt
 *    to avoid further disruption.</li>
 * </ol>
 * <p>
 * Dependencies:
 * <ul>
 *   <li>{@code ObjectMapper}: Used for JSON serialization/deserialization of the failed event.</li>
 *   <li>{@code EventProcessingCoordinator}: Responsible for processing the reprocessed event payload.</li>
 * </ul>
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FailedEventRecoveryService {

    private final ObjectMapper objectMapper;
    private final EventProcessingCoordinator coordinator;

    /**
     * Attempts to recover a failed event by deserializing its raw JSON representation, logging relevant failure details,
     * and reprocessing its payload.
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
     * @param rawFailedEvent the raw JSON string representing the failed event to be recovered
     */
    public void recover(String rawFailedEvent) {
        try {
            FailedEvent failedEvent = objectMapper.readValue(rawFailedEvent, FailedEvent.class);

            log.info("Recovering failed event. originalEventId={}, attemptCount={}, failureType={}, errorCodes={}",
                    failedEvent.originalEventId(),
                    failedEvent.attemptCount(),
                    failedEvent.failureType(),
                    failedEvent.errorCodes()
            );

            String originalEvent = objectMapper.writeValueAsString(failedEvent.payloadMap());

            int nextAttempt = failedEvent.attemptCount() + 1;

            coordinator.processRawEvent(originalEvent, nextAttempt);

        } catch (Exception exception) {
            log.error("Failed to recover failed event. rawFailedEvent={}", rawFailedEvent, exception);
            throw exception;
        }
    }

}