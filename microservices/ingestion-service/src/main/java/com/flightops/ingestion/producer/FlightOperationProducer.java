package com.flightops.ingestion.producer;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Kafka producer responsible for publishing flight operation events to the
 * configured ingestion topic.
 * <p>
 * This component serves as the messaging boundary between the application and
 * the Kafka event streaming platform. It publishes
 * {@link FlightOperationEnvelope} messages containing flight operation events
 * and associated metadata required for downstream processing.
 * <p>
 * Messages are published using the aggregate identifier contained within the
 * envelope as the Kafka message key. This ensures that events associated with
 * the same aggregate are consistently routed to the same partition, preserving
 * event ordering for a given flight.
 * <p>
 * Publication is asynchronous. Each publish operation returns a
 * {@link java.util.concurrent.CompletableFuture} that completes when Kafka
 * acknowledges the message or completes exceptionally if the send fails or
 * times out.
 * <p>
 * Published events may be consumed by downstream services responsible for
 * operational processing, notifications, analytics, auditing, or other
 * event-driven workflows.
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
     * maintain ordering guarantees for events associated with the same aggregate.
     * </p>
     *
     * <p>
     * Publication is asynchronous. The returned {@link CompletableFuture}
     * completes when Kafka acknowledges the send or completes exceptionally if the
     * publish operation fails or times out.
     * </p>
     *
     * @param envelope the event envelope containing flight operation data and
     *                 event metadata to be published; must not be {@code null}
     * @return a {@link CompletableFuture} representing the outcome of the publish
     *         operation
     */
    public CompletableFuture<SendResult<String, FlightOperationEnvelope>> publish(
            FlightOperationEnvelope envelope
    ) {
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

        return kafkaTemplate.send(record)
                .orTimeout(10, TimeUnit.SECONDS)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error(
                                "flight_operation_publish_failed eventId={} correlationId={} aggregateId={} topic={}",
                                envelope.getEventId(),
                                envelope.getCorrelationId(),
                                envelope.getAggregateId(),
                                topic,
                                exception
                        );
                        return;
                    }

                    log.info(
                            "flight_operation_published eventId={} correlationId={} aggregateId={} topic={} partition={} offset={}",
                            envelope.getEventId(),
                            envelope.getCorrelationId(),
                            envelope.getAggregateId(),
                            topic,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset()
                    );
                });
    }
}