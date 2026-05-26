package com.flightops.processing.consumer;

import com.flightops.processing.service.FailedEventRecoveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * A Kafka consumer for retrying failed flight operation events. It listens to a configured retry topic
 * and attempts to recover events that previously failed processing.
 * <p>
 * This class works in coordination with the {@code FailedEventRecoveryService}, which handles the
 * logic for deserialization and re-processing of the failed event data.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FlightOperationRetryConsumer {

    private final FailedEventRecoveryService recoveryService;

    /**
     * Consumes a message from the retry Kafka topic and attempts to recover the failed event.
     *
     * @param rawFailedEvent the raw JSON string representing the failed event that needs recovery
     */
    @KafkaListener(
            topics = "${app.kafka.topics.retry}",
            groupId = "flight-ops-retry-group"
    )
    public void consumeRetry(String rawFailedEvent) {
        recoveryService.recover(rawFailedEvent);
    }

}
