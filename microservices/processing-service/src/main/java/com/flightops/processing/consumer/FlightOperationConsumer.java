package com.flightops.processing.consumer;

import com.flightops.contracts.failure.FailedEvent;
import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.producer.FailureEventProducer;
import com.flightops.processing.service.FlightOperationProcessingService;
import com.flightops.processing.validation.ValidationError;
import com.flightops.processing.validation.ValidationErrorType;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

@Component
@RequiredArgsConstructor
public class FlightOperationConsumer {

    private static final Logger log = LoggerFactory.getLogger(FlightOperationConsumer.class);

    private final ObjectMapper objectMapper;
    private final EventIdempotencyService idempotencyService;
    private final FlightOperationProcessingService processingService;
    private final FailureEventProducer failureEventProducer;

    /**
     * Consumes and processes a raw Kafka message from the ingestion topic.
     *
     * <p>The message is deserialized into an {@code EventEnvelopeJson}, claimed for
     * idempotent processing, transformed into a {@code FlightOperationEvent}, and
     * delegated to the processing service.</p>
     *
     * <p>If validation fails, the event claim is released and the message is routed
     * either to a retry topic or dead-letter topic depending on whether the
     * validation errors are retryable.</p>
     *
     * <p>If an unexpected failure occurs during processing, the event claim is
     * released and the failure is logged.</p>
     *
     * @param rawMessage the raw Kafka message payload to consume
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ingestion}",
            groupId = "flight-ops-processing-group"
    )
    public void consume(String rawMessage) {
        EventEnvelopeJson envelope = null;

        try {
            envelope = readEnvelope(rawMessage);

            if (!tryClaimEvent(envelope)) {
                return;
            }

            processEvent(envelope);

        } catch (FlightOperationValidationException exception) {
            handleValidationFailure(envelope, exception, rawMessage);

        } catch (Exception exception) {
            handleUnexpectedFailure(envelope, rawMessage, exception);
        }
    }

    private void processEvent(EventEnvelopeJson envelope) {
        FlightOperationEvent payload = toFlightOperationEvent(envelope);

        processingService.process(envelope, payload);

        idempotencyService.markProcessed(envelope.eventId());

        logProcessingSuccess(envelope);
    }

    private void handleValidationFailure(
            EventEnvelopeJson envelope,
            FlightOperationValidationException exception,
            String rawMessage
    ) {
        idempotencyService.releaseClaim(exception.eventId());

        FailedEvent failedEvent = buildFailedEvent(envelope, exception, rawMessage);

        if (retryable(exception)) {
            failureEventProducer.sendToRetry(failedEvent);

            log.warn(
                    "Validation failed with retryable errors. Routed to retry topic. eventId={}, errors={}",
                    exception.eventId(),
                    exception.errors()
            );

            return;
        }

        failureEventProducer.sendToDlq(failedEvent);

        log.warn(
                "Validation failed with non-retryable errors. Routed to DLQ. eventId={}, errors={}",
                exception.eventId(),
                exception.errors()
        );
    }

    private boolean retryable(FlightOperationValidationException exception) {
        return exception.errors().stream()
                .anyMatch(error -> error.type() == ValidationErrorType.RETRYABLE);
    }

    private void handleUnexpectedFailure(
            EventEnvelopeJson envelope,
            String rawMessage,
            Exception exception
    ) {
        if (envelope != null) {
            idempotencyService.releaseClaim(envelope.eventId());

            log.error(
                    "Failed to process event. eventId={}, rawMessage={}",
                    envelope.eventId(),
                    rawMessage,
                    exception
            );

            return;
        }

        log.error(
                "Failed to parse raw message. rawMessage={}",
                rawMessage,
                exception
        );
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

    private FailedEvent buildFailedEvent(
            EventEnvelopeJson envelope,
            FlightOperationValidationException exception,
            String rawMessage) {

        boolean retryable = retryable(exception);

        return new FailedEvent(
                envelope.eventId(),
                envelope.eventType().name(),
                envelope.aggregateId(),
                envelope.correlationId(),
                retryable ? "RETRYABLE" : "NON_RETRYABLE",
                exception.errors().stream()
                        .map(ValidationError::code)
                        .toList(),
                exception.getMessage(),
                rawMessage,
                Instant.now()
        );
    }

}