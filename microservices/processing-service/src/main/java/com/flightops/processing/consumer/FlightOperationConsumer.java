package com.flightops.processing.consumer;

import com.flightops.processing.service.EventProcessingCoordinator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * A Kafka consumer for flight operation events received from the primary
 * ingestion topic. It listens to the configured ingestion topic and
 * delegates incoming events for processing.
 * <p>
 * This class works in coordination with the
 * {@code EventProcessingCoordinator}, which handles event validation,
 * routing, and processing workflows.
 * <p>
 * Events consumed from the ingestion topic are treated as first-attempt
 * processing operations and are therefore processed with an initial
 * attempt count of {@code 1}.
 * <p>
 * Offsets are manually acknowledged only after successful processing.
 * If processing fails, the offset is not acknowledged and the exception
 * is propagated to allow the configured error handling and retry
 * mechanisms to take effect.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private final EventProcessingCoordinator coordinator;

    /*
     * Consumes a raw Kafka message from the ingestion topic and delegates it
     * to the {@code EventProcessingCoordinator} for processing.
     * <p>
     * Events consumed from the primary ingestion topic are treated as first-attempt
     * processing operations and therefore begin with an attempt count of {@code 1}.
     * <p>
     * The Kafka offset is acknowledged only after successful processing. If
     * processing fails, the offset is not acknowledged and the exception is
     * rethrown to allow the configured error handling and retry mechanisms to
     * take effect.
     *
     * @param rawMessage the raw JSON string representing the event to be processed
     * @param acknowledgment the acknowledgment handle used for manual Kafka
     *        offset commits after successful processing
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage, Acknowledgment acknowledgment) {
        try {
            coordinator.processRawEvent(rawMessage, 1);
            acknowledgment.acknowledge();
            log.info("Kafka offset acknowledged for event: {}", rawMessage);
        } catch (Exception exception) {
            log.error("Failed to process event; offset will not be acknowledged. Event: {}", rawMessage, exception);
            throw exception;
        }
    }

}