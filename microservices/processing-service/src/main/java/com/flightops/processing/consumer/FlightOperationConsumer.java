package com.flightops.processing.consumer;

import com.flightops.processing.service.EventProcessingCoordinator;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlightOperationConsumer.class);

    private final EventProcessingCoordinator coordinator;


    /**
     * Consumes a raw Kafka message from the ingestion topic and delegates it
     * to the {@code EventProcessingCoordinator} for processing.
     *
     * @param rawMessage the raw JSON string representing the event to be processed
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage) {
        coordinator.processRawEvent(rawMessage);
    }

}