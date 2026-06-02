package com.flightops.processing.consumer;

import com.flightops.processing.service.FailedEventRecoveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * A Kafka consumer for retrying failed flight operation events. It listens to
 * a configured retry topic and attempts to recover events that previously
 * failed to process.
 * <p>
 * This class works in coordination with the {@code FailedEventRecoveryService},
 * which handles the logic for deserialization and re-processing of the failed
 * event data.
 * <p>
 * Offsets are manually acknowledged only after successful recovery to ensure
 * failed events remain eligible for retry handling.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FlightOperationRetryConsumer {

    private final FailedEventRecoveryService recoveryService;

    /**
     * Consumes a message from the retry Kafka topic and attempts to recover the
     * failed event.
     * <p>
     * The Kafka offset is acknowledged only after the failed event has been
     * successfully recovered. If recovery fails, the offset is not acknowledged
     * and the exception is rethrown to allow the configured error handling and
     * retry mechanisms to take effect.
     *
     * @param rawFailedEvent the raw JSON string representing the failed event
     *        that needs recovery
     * @param acknowledgment the acknowledgment handle used for manual Kafka
     *        offset commits after successful recovery
     */
    @KafkaListener(
            topics = "${app.kafka.topics.retry}",
            groupId = "flight-ops-retry-group"
    )
    public void consumeRetry(String rawFailedEvent, Acknowledgment acknowledgment) {
        try {
            recoveryService.recover(rawFailedEvent);
            acknowledgment.acknowledge();
            log.info("Kafka offset acknowledged for retry event: {}", rawFailedEvent);
        } catch (Exception exception) {
            log.error("Failed to recover retry event; offset will not be acknowledged. Event: {}", rawFailedEvent, exception);
            throw exception;
        }

    }

}
