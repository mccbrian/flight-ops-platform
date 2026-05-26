package com.flightops.processing.producer;

import com.flightops.contracts.failure.FailedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Produces failure events and routes them to specific Kafka topics.
 *
 * <p>This class encapsulates the logic for sending events to either a retry topic
 * or a dead-letter queue (DLQ). It ensures that the event data is properly serialized
 * as JSON before dispatching.
 */
@Component
@RequiredArgsConstructor
public class FailureEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.retry}")
    private String retryTopic;

    @Value("${app.kafka.topics.dlq}")
    private String dlqTopic;

    /**
     * Sends a failed event to the configured Kafka retry topic.
     *
     * @param event the {@code FailedEvent} object containing metadata and details about
     *              the failed event that needs to be retried
     */
    public void sendToRetry(FailedEvent event) {
        send(retryTopic, event);
    }

    /**
     * Sends a failed event to the dead-letter queue (DLQ) topic for further inspection or
     * manual intervention. This is typically used when the event failure is deemed non-retryable.
     *
     * @param event the failed event containing diagnostic information and metadata about the
     *              original event that could not be processed
     */
    public void sendToDlq(FailedEvent event) {
        send(dlqTopic, event);
    }

    private void send(String topic, FailedEvent event) {
        try {
            kafkaTemplate.send(
                    topic,
                    event.originalEventId().toString(),
                    objectMapper.writeValueAsString(event)
            );
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to serialize FailedEvent", exception);
        }
    }

}