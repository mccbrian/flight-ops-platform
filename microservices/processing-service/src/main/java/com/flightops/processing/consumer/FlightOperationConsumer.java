package com.flightops.processing.consumer;

import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.service.FlightOperationProcessingService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlightOperationConsumer.class);

    private final ObjectMapper objectMapper;
    private final EventIdempotencyService idempotencyService;
    private final FlightOperationProcessingService processingService;

    /**
     * Consumes a raw message from the specified Kafka topic and processes it.
     * This method attempts to parse the raw message into an {@code EventEnvelopeJson},
     * validates its uniqueness, transforms it into a domain-specific {@code FlightOperationEvent},
     * and processes it using the appropriate services.
     * If the processing fails, an error handling mechanism is invoked.
     *
     * @param rawMessage The raw Kafka message payload to be consumed and processed.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage) {
        EventEnvelopeJson envelope = null;

        try {
            envelope = readEnvelope(rawMessage);
            var eventId = envelope.eventId();

            if (!tryClaimEvent(envelope)) {
                return;
            }

            FlightOperationEvent payload = toFlightOperationEvent(envelope);
            processingService.process(envelope, payload);
            idempotencyService.markProcessed(eventId);

            logProcessingSuccess(envelope);
        } catch (Exception ex) {
            handleConsumptionFailure(rawMessage, envelope, ex);
        }
    }

    private EventEnvelopeJson readEnvelope(String rawMessage) throws Exception {
        return objectMapper.readValue(rawMessage, EventEnvelopeJson.class);
    }

    private boolean tryClaimEvent(EventEnvelopeJson envelope) {
        if (!idempotencyService.claimForProcessing(envelope.eventId())) {
            log.info("Event already claimed or being processed. eventId={}, aggregateId={}",
                    envelope.eventId(), envelope.aggregateId());
            return false;
        }
        return true;
    }

    private FlightOperationEvent toFlightOperationEvent(EventEnvelopeJson envelope) {
        return objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class);
    }

    private void logProcessingSuccess(EventEnvelopeJson envelope) {
        log.info("Event processed successfully. eventId={}, aggregateId={}",
                envelope.eventId(), envelope.aggregateId());
    }

    private void handleConsumptionFailure(String rawMessage, EventEnvelopeJson envelope, Exception ex) {
        if (envelope == null) {
            log.error("Failed to parse raw message. rawMessage={}", rawMessage, ex);
            return;
        }

        idempotencyService.releaseClaim(envelope.eventId());
        log.error("Failed to process event. eventId={}, rawMessage={}",
                envelope.eventId(), rawMessage, ex);
    }

}