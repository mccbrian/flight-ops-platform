package com.flightops.ingestion.service;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.ingestion.dto.FlightOperationRequest;
import com.flightops.ingestion.producer.FlightOperationProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Service responsible for ingesting flight operation requests and publishing
 * them as standardized event messages.
 * <p>
 * This service acts as the boundary between the API layer and the event
 * streaming infrastructure. It transforms incoming
 * {@link FlightOperationRequest} objects into Avro-based
 * {@link FlightOperationEvent} payloads and wraps them in a
 * {@link FlightOperationEnvelope} containing metadata required for event
 * processing, correlation, tracing, and auditing.
 * </p>
 *
 * <p>
 * For every ingestion request:
 * </p>
 * <ol>
 *     <li>A {@link FlightOperationEvent} payload is created from the request.</li>
 *     <li>Unique event and correlation identifiers are generated.</li>
 *     <li>The payload is wrapped in a {@link FlightOperationEnvelope}.</li>
 *     <li>The envelope is published through the configured
 *     {@link FlightOperationProducer}.</li>
 * </ol>
 *
 * <p>
 * The generated envelope provides a consistent event contract that can be
 * consumed by downstream services for processing flight status changes,
 * operational updates, notifications, analytics, and audit workflows.
 * </p>
 */
@Service
@RequiredArgsConstructor
public class FlightOperationIngestionService {

    private final FlightOperationProducer producer;

    /**
     * Ingests a flight operation request and publishes it as an event.
     * <p>
     * The supplied request is transformed into a
     * {@link FlightOperationEvent}, which contains the business-specific
     * details of the flight operation. The event is then wrapped inside a
     * {@link FlightOperationEnvelope} containing metadata used for event
     * routing, correlation, observability, and traceability.
     * </p>
     *
     * <p>
     * During processing, the method:
     * </p>
     * <ul>
     *     <li>Captures the current timestamp for the event payload.</li>
     *     <li>Generates a unique event identifier.</li>
     *     <li>Generates a correlation identifier for distributed tracing.</li>
     *     <li>Creates a standardized event envelope.</li>
     *     <li>Publishes the resulting event through the configured producer.</li>
     * </ul>
     *
     * <p>
     * The resulting event can be consumed by downstream systems to react to
     * operational flight changes such as delays, gate updates, cancellations,
     * status transitions, or other operational activities.
     * </p>
     *
     * @param request the incoming flight operation request containing the
     *                flight identifier and operation details to be published
     *                as an event; must not be {@code null}
     */
    public void ingest(FlightOperationRequest request) {
        FlightOperationEvent payload = FlightOperationEvent.newBuilder()
                .setFlightId(request.flightId())
                .setOperationType(request.operationType().name())
                .setStatus(request.status())
                .setGate(request.gate())
                .setDelayMinutes(request.delayMinutes())
                .setReason(request.reason())
                .setEventTime(Instant.now())
                .build();

        String eventId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();

        FlightOperationEnvelope envelope = FlightOperationEnvelope.newBuilder()
                .setEventId(eventId)
                .setEventType("FLIGHT_OPERATION_EVENT")
                .setAggregateId(String.valueOf(request.flightId()))
                .setCorrelationId(correlationId)
                .setOccurredAt(Instant.now())
                .setPayload(payload)
                .build();

        producer.publish(envelope);
    }

}