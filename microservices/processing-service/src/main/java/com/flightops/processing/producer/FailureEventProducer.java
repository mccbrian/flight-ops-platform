package com.flightops.processing.producer;

import com.flightops.contracts.failure.FailedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class FailureEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.retry}")
    private String retryTopic;

    @Value("${app.kafka.topics.dlq}")
    private String dlqTopic;

    public void sendToRetry(FailedEvent event) {
        kafkaTemplate.send(
                retryTopic,
                event.originalEventId().toString(),
                event.toString()
        );
    }

    public void sendToDlq(FailedEvent event) {
        kafkaTemplate.send(
                dlqTopic,
                event.originalEventId().toString(),
                event.toString()
        );
    }

}