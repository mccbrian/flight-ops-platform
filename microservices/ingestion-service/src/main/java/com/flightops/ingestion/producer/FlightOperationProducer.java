package com.flightops.ingestion.producer;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Kafka producer responsible for publishing flight operation events to the
 * configured ingestion topic.
 *
 * <p>
 * This component serves as the messaging boundary between the application and
 * the Kafka event streaming platform. It publishes
 * {@link FlightOperationEnvelope} messages containing flight operation events
 * and associated metadata required for downstream processing.
 * </p>
 *
 * <p>
 * Messages are published using the aggregate identifier contained within the
 * envelope as the Kafka message key. This ensures that events associated with
 * the same aggregate are consistently routed to the same partition, preserving
 * event ordering for a given flight.
 * </p>
 *
 * <p>
 * Published events may be consumed by downstream services responsible for
 * operational processing, notifications, analytics, auditing, or other
 * event-driven workflows.
 * </p>
 */
@Slf4j
@Component
public class FlightOperationProducer {

    private final KafkaTemplate<String, FlightOperationEnvelope> kafkaTemplate;
    private final String topic;

    public FlightOperationProducer(
            KafkaTemplate<String, FlightOperationEnvelope> kafkaTemplate,
            @Value("${app.kafka.topics.ingestion}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Publishes a flight operation event envelope to Kafka.
     *
     * <p>
     * The envelope's aggregate identifier is used as the Kafka message key to
     * maintain ordering guarantees for events associated with the same
     * aggregate.
     * </p>
     *
     * @param envelope the event envelope containing flight operation data and
     *                 event metadata to be published; must not be {@code null}
     */
    public void publish(FlightOperationEnvelope envelope) {
        log.info(
                "flight_operation_publish_requested eventId={}, correlationId={}, aggregateId={}, topic={}",
                envelope.getEventId(),
                envelope.getCorrelationId(),
                envelope.getAggregateId(),
                topic
        );

        ProducerRecord<String, FlightOperationEnvelope> record =
                new ProducerRecord<>(topic, envelope.getAggregateId(), envelope);

        record.headers().add(
                "X-Correlation-Id",
                envelope.getCorrelationId().getBytes(StandardCharsets.UTF_8)
        );

        record.headers().add(
                "X-Event-Id",
                envelope.getEventId().getBytes(StandardCharsets.UTF_8)
        );

        kafkaTemplate.send(record);
    }

}