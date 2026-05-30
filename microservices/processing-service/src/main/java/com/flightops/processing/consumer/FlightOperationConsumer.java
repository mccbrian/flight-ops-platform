package com.flightops.processing.consumer;

import com.flightops.processing.service.EventProcessingCoordinator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private final EventProcessingCoordinator coordinator;


    /**
     * Consumes a raw Kafka message from the ingestion topic and delegates it
     * to the {@code EventProcessingCoordinator} for processing.
     * <p>
     * Events consumed from the primary ingestion topic are treated as first-attempt
     * processing operations and therefore begin with an attempt count of {@code 1}.
     *
     * @param rawMessage the raw JSON string representing the event to be processed
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage) {
        try {
            coordinator.processRawEvent(rawMessage, 1);
        } catch (Exception exception) {
            log.error("Failed to process raw event: {}", rawMessage, exception);
            throw exception;
        }
    }

}