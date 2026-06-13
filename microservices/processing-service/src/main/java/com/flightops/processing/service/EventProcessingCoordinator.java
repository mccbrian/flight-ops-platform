package com.flightops.processing.service;

import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.component.FailureRouter;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.metrics.FlightOperationMetrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Coordinates the end-to-end processing lifecycle of flight operation events.
 *
 * <p>This service acts as the orchestration layer for inbound event processing.
 * It is responsible for:</p>
 *
 * <ul>
 *   <li>Deserializing raw event messages into domain objects</li>
 *   <li>Enforcing idempotent processing through the event claim store</li>
 *   <li>Delegating business processing to the flight operation processing service</li>
 *   <li>Marking successfully processed events in the idempotency store</li>
 *   <li>Classifying processing failures and determining retry eligibility</li>
 *   <li>Routing failed events to either retry or dead-letter topics</li>
 *   <li>Recording operational metrics and processing latency</li>
 * </ul>
 *
 * <p>Events are processed at most once through a claim-and-complete workflow.
 * Duplicate or previously processed events are detected through the
 * {@link EventIdempotencyService} and are not processed again.</p>
 *
 * <p>When processing fails, retryable failures may be routed to a retry topic
 * until the configured maximum attempt count is reached. Non-retryable failures,
 * or failures that have exhausted their retry attempts, are routed to a
 * dead-letter queue (DLQ) for further investigation.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessingCoordinator {

    private final FlightOperationMetrics metrics;
    private final ObjectMapper objectMapper;
    private final EventIdempotencyService idempotencyService;
    private final FlightOperationProcessingService processingService;
    private final FailureRouter failureRouter;

    /**
     * Processes an event envelope through the standard flight operation processing workflow.
     * <p>
     * The event is first claimed through the idempotency service to prevent duplicate processing. If the event has
     * already been claimed or processed, it is ignored and no further action is taken.
     * <p>
     * Successfully claimed events are converted to a {@code FlightOperationEvent} and delegated to the
     * {@code FlightOperationProcessingService} for business processing. Upon successful completion, the event is marked
     * as processed in the idempotency store and success metrics are recorded.
     * <p>
     * Validation failures and unexpected processing exceptions are classified and routed to either a retry topic or a
     * dead-letter queue (DLQ) based on retry eligibility and the current attempt count. Processing metrics and latency
     * measurements are recorded regardless of outcome.
     *
     * @param envelope the event envelope containing event metadata and payload
     * @param attemptCount the current processing attempt count for the event, starting at {@code 1} for initial ingestion
     *                     and incremented for each retry attempt
     */
    public void processEnvelope(EventEnvelopeJson envelope, int attemptCount) {
        Timer.Sample sample = metrics.startProcessingTimer();

        try {
            if (!claimEvent(envelope)) {
                return;
            }

            FlightOperationEvent payload = objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class);

            processingService.process(envelope, payload);

            idempotencyService.markProcessed(envelope.eventId());

            metrics.incrementProcessed();

            log.info("event_processed eventId={}, correlationId={}, aggregateId={}, flightId={}, operationType={}",
                    envelope.eventId(),
                    envelope.correlationId(),
                    envelope.aggregateId(),
                    envelope.payload().flightId(),
                    envelope.payload().operationType());

        } catch (FlightOperationValidationException exception) {
            failureRouter.routeValidationFailure(envelope, attemptCount, exception);

        } catch (Exception exception) {
            failureRouter.handleUnexpectedFailure(envelope, attemptCount, exception);

        } finally {
            metrics.stopProcessingTimer(sample);
        }
    }

    /**
     * Processes a raw event message by parsing it and delegating it through the standard event processing workflow,
     * including idempotency checks, validation, business processing, metrics recording, and failure handling.
     * <p>
     * If processing succeeds, the event is marked as processed in the idempotency store and success metrics are recorded.
     * <p>
     * If processing fails, the event may be routed to either a retry topic or a dead-letter queue (DLQ) depending on the
     * type of failure, retry eligibility, and the current retry attempt count.
     * <p>
     * Duplicate or previously claimed events are detected through the idempotency service and are ignored without
     * further processing.
     *
     * @param rawMessage the raw event message received as a JSON string
     * @param attemptCount the current processing attempt count for the event, starting at {@code 1} for initial ingestion
     *                     and incremented for each retry attempt
     */
    public void processRawEvent(String rawMessage, int attemptCount) {
        EventEnvelopeJson envelope = readEnvelope(rawMessage);
        processEnvelope(envelope, attemptCount);
    }

    private EventEnvelopeJson readEnvelope(String rawMessage) {
        return objectMapper.readValue(rawMessage, EventEnvelopeJson.class);
    }

    private boolean claimEvent(EventEnvelopeJson envelope) {
        if (!idempotencyService.claimForProcessing(envelope.eventId())) {

            metrics.incrementDuplicate();

            log.info(
                    "Event already claimed or processed. eventId={}, correlationId={}, aggregateId={}, flightId={}, operationType={}",
                    envelope.eventId(),
                    envelope.correlationId(),
                    envelope.aggregateId(),
                    envelope.payload().flightId(),
                    envelope.payload().operationType()
            );

            return false;
        }

        return true;
    }

}