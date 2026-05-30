package com.flightops.processing.producer;

import com.flightops.contracts.failure.FailedEvent;
import com.flightops.processing.exception.PublishFailureEventException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

/**
 * Produces failure events and routes them to specific Kafka topics.
 *
 * <p>This class encapsulates the logic for sending events to either a retry topic
 * or a dead-letter queue (DLQ). It ensures that the event data is properly serialized
 * as JSON before dispatching.
 */
@Slf4j
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

    private void send(String topic, FailedEvent failedEvent) {
        try {
            SendResult<String, String> result = kafkaTemplate
                    .send(topic, failedEvent.originalEventId().toString(), objectMapper.writeValueAsString(failedEvent))
                    .get(10, TimeUnit.SECONDS);

            log.info("Published failed event. topic={}, originalEventId={}, partition={}, offset={}",
                    topic,
                    failedEvent.originalEventId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

        } catch (Exception exception) {
            throw new PublishFailureEventException(
                    "Failed to publish failed event to topic=" + topic +
                            ", originalEventId=" + failedEvent.originalEventId(),
                    exception
            );
        }
    }

}