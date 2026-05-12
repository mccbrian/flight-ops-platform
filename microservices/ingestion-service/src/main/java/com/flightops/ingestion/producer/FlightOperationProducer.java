package com.flightops.ingestion.producer;

import com.flightops.contracts.envelope.EventEnvelope;
import com.flightops.contracts.ingestion.FlightOperationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class FlightOperationProducer {

    private final KafkaTemplate<String, EventEnvelope<FlightOperationEvent>> kafkaTemplate;
    private final String topic;

    public FlightOperationProducer(
            KafkaTemplate<String, EventEnvelope<FlightOperationEvent>> kafkaTemplate,
            @Value("${app.kafka.topics.ingestion}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(EventEnvelope<FlightOperationEvent> envelope) {
        kafkaTemplate.send(topic, envelope.aggregateId(), envelope);
    }

}
