package com.flightops.processing.consumer;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.service.FailedEventRecoveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * A Kafka consumer for retrying failed flight operation events. It listens to a configured retry topic and attempts to
 * recover events that previously failed to process.
 * <p>
 * This class works in coordination with the {@code FailedEventRecoveryService}, which handles the logic for deserialization
 * and re-processing of the failed event data.
 * <p>
 * Offsets are manually acknowledged only after successful recovery to ensure failed events remain eligible for retry handling.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FlightOperationRetryConsumer {

    private final FailedEventRecoveryService recoveryService;

    /**
     * Consumes a message from the retry Kafka topic and attempts to recover the failed event.
     * <p>
     * The Kafka offset is acknowledged only after the failed event has been successfully recovered. If recovery fails,
     * the offset is not acknowledged and the exception is rethrown to allow the configured error handling and
     * retry mechanisms to take effect.
     *
     * @param failedEvent the {@code FailedEvent} object representing the failed event to be recovered
     * @param acknowledgment the acknowledgment handle used for manual Kafka offset commits after successful recovery
     */
    @KafkaListener(
            topics = "${app.kafka.topics.retry}",
            groupId = "flight-ops-retry-group"
    )
    public void consumeRetry(FailedEvent failedEvent, Acknowledgment acknowledgment) {
        try {
            log.info(
                    "failed_event_received originalEventId={}, correlationId={}, aggregateId={}, attemptCount={}",
                    failedEvent.getOriginalEventId(),
                    failedEvent.getCorrelationId(),
                    failedEvent.getAggregateId(),
                    failedEvent.getAttemptCount()
            );
            recoveryService.recover(failedEvent);
            acknowledgment.acknowledge();
            log.info("Kafka offset acknowledged for retry event. originalEventId={}, aggregateId={}, attemptCount={}",
                    failedEvent.getOriginalEventId(),
                    failedEvent.getAggregateId(),
                    failedEvent.getAttemptCount());
        } catch (Exception exception) {
            log.error("Failed to recover retry event. Offset will not be acknowledged. originalEventId={}",
                    failedEvent == null ? null : failedEvent.getOriginalEventId(),
                    exception);

            throw exception;
        }
    }

}