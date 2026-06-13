package com.flightops.processing.producer;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.exception.PublishFailureEventException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Produces failure events and routes them to specific Kafka topics.
 * <p>
 * This class encapsulates the logic for sending events to either a retry topic or a dead-letter queue (DLQ). It ensures
 * that event data is serialized before dispatch and published using the original aggregate identifier as the Kafka message
 * key.
 * <p>
 * Using the aggregate identifier as the message key preserves partition affinity and maintains event ordering for events
 * belonging to the same aggregate across ingestion, retry, and dead-letter processing flows.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FailureEventProducer {

    private final KafkaTemplate<String, FailedEvent> kafkaTemplate;

    @Value("${app.kafka.topics.retry}")
    private String retryTopic;

    @Value("${app.kafka.topics.dlq}")
    private String dlqTopic;

    /**
     * Sends a failed event to the configured Kafka retry topic.
     *
     * @param failedEvent the {@code FailedEvent} object containing metadata and details about the failed event that
     *                    needs to be retried
     */
    public void sendToRetry(FailedEvent failedEvent) {
        send(retryTopic, failedEvent);
    }

    /**
     * Sends a failed event to the dead-letter queue (DLQ) topic for further inspection or manual intervention. This is
     * typically used when the event failure is deemed non-retryable.
     *
     * @param event the failed event containing diagnostic information and metadata about the
     *              original event that could not be processed
     */
    public void sendToDlq(FailedEvent event) {
        send(dlqTopic, event);
    }

    private void send(String topic, FailedEvent failedEvent) {
        try {
            SendResult<String, FailedEvent> result = kafkaTemplate
                    .send(topic, failedEvent.getAggregateId(), failedEvent)
                    .get(10, TimeUnit.SECONDS);

            log.info("Published failed event. topic={}, originalEventId={}, aggregateId={}, partition={}, offset={}",
                    topic,
                    failedEvent.getOriginalEventId(),
                    failedEvent.getAggregateId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

        } catch (Exception exception) {
            throw new PublishFailureEventException(
                    "Failed to publish failed event to topic=" + topic +
                            ", originalEventId=" + failedEvent.getOriginalEventId(),
                    exception
            );
        }
    }

}