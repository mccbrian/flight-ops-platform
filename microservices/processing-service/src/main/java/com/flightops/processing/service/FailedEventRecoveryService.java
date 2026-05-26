package com.flightops.processing.service;

import com.flightops.contracts.failure.FailedEvent;
import com.flightops.processing.consumer.FlightOperationConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Service to handle the recovery of failed events. This service processes events
 * that could not be handled successfully during their initial processing and
 * attempts to recover their processing by consuming them again.
 * <p>
 * The recovery process involves:
 * 1. Converting a raw failed event JSON string into a {@code FailedEvent} object.
 * 2. Logging relevant details about the failed event, such as its original ID, failure type, and error codes.
 * 3. Extracting the payload of the failed event and passing it to a consumer for reprocessing.
 * 4. Handling and logging any exceptions that occur during the recovery attempt to avoid further disruption.
 * <p>
 * Dependencies:
 * - {@code ObjectMapper}: Used for JSON serialization/deserialization of the failed event.
 * - {@code FlightOperationConsumer}: Responsible for consuming the reprocessed event payload.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FailedEventRecoveryService {

    private final ObjectMapper objectMapper;
    private final FlightOperationConsumer flightOperationConsumer;

    /**
     * Attempts to recover a failed event by deserializing its raw JSON representation,
     * logging relevant failure details, and reprocessing its payload.
     * <p>
     * The recovery process includes:
     * - Parsing the raw JSON string into a {@link FailedEvent} object.
     * - Logging the original event ID, failure type, and associated error codes.
     * - Extracting the event payload and passing it to the {@link FlightOperationConsumer} for reprocessing.
     * - Handling and logging exceptions that occur during the recovery attempt.
     *
     * @param rawFailedEvent the raw JSON string representing the failed event to be recovered
     */
    public void recover(String rawFailedEvent) {
        try {
            FailedEvent failedEvent =
                    objectMapper.readValue(rawFailedEvent, FailedEvent.class);

            log.info("Recovering failed event. originalEventId={}, failureType={}, errorCodes={}",
                    failedEvent.originalEventId(),
                    failedEvent.failureType(),
                    failedEvent.errorCodes());

            String rawPayload = objectMapper.writeValueAsString(failedEvent.payloadMap());
            flightOperationConsumer.consume(rawPayload);

        } catch (Exception exception) {
            log.error("Failed to recover failed event. rawFailedEvent={}",
                    rawFailedEvent,
                    exception);
        }
    }

}