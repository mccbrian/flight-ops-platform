package com.flightops.processing.consumer;

import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.idempotency.EventIdempotencyService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.TreeNode;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlightOperationConsumer.class);

    private final EventIdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage) {
        try {
            EventEnvelopeJson envelope = objectMapper.readValue(rawMessage, EventEnvelopeJson.class);

            if (!idempotencyService.firstTimeSeen(envelope.eventId())) {
                log.info("Duplicate event skipped. eventId={}, aggregateId={}",
                        envelope.eventId(), envelope.aggregateId());
                return;
            }

            log.info("Envelope received. eventId={}, type={}, aggregateId={}",
                    envelope.eventId(),
                    envelope.eventType(),
                    envelope.aggregateId());

            FlightOperationEvent payload =
                    objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class);

            log.info("Parsed payload. flightId={}, operationType={}, status={}",
                    payload.flightId(),
                    payload.operationType(),
                    payload.status());

        } catch (Exception exception) {
            log.error("Failed to parse message: {}", rawMessage, exception);
        }



    }

}