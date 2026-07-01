package com.flightops.processing.consumer;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.mapper.FlightOperationMapper;
import com.flightops.processing.service.EventProcessingCoordinator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for flight operation events received from the primary ingestion topic.
 * <p>
 * Consumed {@link FlightOperationEnvelope} records are mapped to the internal {@link EventEnvelopeJson} representation
 * and delegated to the {@link EventProcessingCoordinator} for processing.
 * <p>
 * Events received from the ingestion topic are treated as first-attempt processing operations and are therefore processed
 * with an initial attempt count of {@code 1}.
 * <p>
 * Kafka offsets are manually acknowledged only after successful processing. If processing fails, the offset is not
 * acknowledged and the exception is propagated so that the configured error handling and retry mechanisms can take effect.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private final FlightOperationMapper mapper;
    private final EventProcessingCoordinator coordinator;

    /**
     * Consumes a flight operation event from the ingestion topic, maps the incoming Avro envelope to the internal event
     * representation, and delegates processing to the {@code EventProcessingCoordinator}.
     * <p>
     * Events consumed from the primary ingestion topic are treated as first-attempt processing operations and therefore
     * begin with an attempt count of {@code 1}.
     * <p>
     * The Kafka offset is acknowledged only after successful processing. If processing fails, the offset is not
     * acknowledged and the exception is rethrown to allow the configured error handling and retry mechanisms to take effect.
     *
     * @param avroEnvelope the deserialized Avro flight operation event received from Kafka
     * @param acknowledgment the acknowledgment handle used for manual offset commits after successful processing
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(FlightOperationEnvelope avroEnvelope, Acknowledgment acknowledgment) {
        try {
            log.info(
                    "flight_operation_received eventId={}, correlationId={}, aggregateId={}",
                    avroEnvelope.getEventId(),
                    avroEnvelope.getCorrelationId(),
                    avroEnvelope.getAggregateId()
            );
            EventEnvelopeJson envelope = mapper.toEventEnvelopeJson(avroEnvelope);
            coordinator.processEnvelope(envelope, 1);
            acknowledgment.acknowledge();
            log.info("Kafka offset acknowledged for event: {}", envelope.eventId());
        } catch (Exception exception) {
            log.error("Failed to process event; offset will not be acknowledged. Event: {}",
                    avroEnvelope == null ? null : avroEnvelope.getEventId(),
                    exception);

            throw exception;
        }
    }

}