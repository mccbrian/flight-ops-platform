package com.flightops.processing.consumer;

import com.flightops.contracts.envelope.EventEnvelope;
import com.flightops.processing.idempotency.EventIdempotencyService;
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

    private final EventIdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage) {
        EventEnvelope envelope = objectMapper.readValue(rawMessage, EventEnvelope.class);

        if (!idempotencyService.firstTimeSeen(envelope.eventId())) {
            log.info("Duplicate event skipped. eventId={}, aggregateId={}",
                    envelope.eventId(), envelope.aggregateId());
        }
    }

}