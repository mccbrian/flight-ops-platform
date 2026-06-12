package com.flightops.processing.service;

import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.processing.domain.ProcessedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.repository.FlightOperationStatusRepository;
import com.flightops.processing.repository.ProcessedEventRepository;
import com.flightops.processing.validation.FlightOperationValidator;
import com.flightops.processing.validation.ValidationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Orchestrates the business processing of flight operation events.
 * <p>
 * This service is responsible for applying validated flight operation events to the domain model and ensuring idempotent
 * persistence of both flight state changes and processed event records.
 * <p>
 * The processing workflow includes:
 * <ul>
 *   <li>Validating the event payload using business validation rules</li>
 *   <li>Rejecting invalid events by throwing a {@link FlightOperationValidationException}</li>
 *   <li>Enforcing idempotency at the database level using the processed event store</li>
 *   <li>Updating existing {@link FlightOperationStatus} records when present</li>
 *   <li>Creating new {@link FlightOperationStatus} records when absent</li>
 *   <li>Recording successfully processed events in the {@link ProcessedEventRepository}</li>
 * </ul>
 * This service does not handle retry logic, error routing, or messaging concerns. Those responsibilities are delegated
 * to higher-level orchestration components.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FlightOperationProcessingService {

    private final FlightOperationValidator validator;
    private final ProcessedEventRepository processedEventRepository;
    private final FlightOperationStatusRepository statusRepository;

    /**
     * Processes a validated flight operation event and applies it to the domain model.
     * <p>
     * This method performs the following steps:
     * <ul>
     *   <li>Validates the event payload using business validation rules</li>
     *   <li>Rejects invalid events by throwing {@link FlightOperationValidationException}</li>
     *   <li>Checks for duplicate processing using the processed event store</li>
     *   <li>Updates or creates {@link FlightOperationStatus} based on flight ID</li>
     *   <li>Persists the updated flight status</li>
     *   <li>Records the event in the processed event repository for idempotency</li>
     * </ul>
     * @param envelope metadata and identifiers for the event
     * @param payload the validated flight operation event payload
     * @throws FlightOperationValidationException if the payload fails, business validation rules
     */
    @Transactional
    public void process(EventEnvelopeJson envelope, FlightOperationEvent payload) {

        ValidationResult result = validator.validate(payload);

        if (!result.valid()) {
            throw new FlightOperationValidationException(envelope.eventId(), result.errors());
        }

        if (processedEventRepository.existsById(envelope.eventId())) {
            log.info("Duplicate event detected at database level. eventId={}", envelope.eventId());
            return;
        }

        FlightOperationStatus status = statusRepository.findById(payload.flightId())
                .map(existing -> {
                    existing.update(payload.status(), payload.gate(), payload.delayMinutes());
                    return existing;
                })
                .orElseGet(() -> new FlightOperationStatus(
                        payload.flightId(),
                        payload.status(),
                        payload.gate(),
                        payload.delayMinutes()
                ));

        statusRepository.save(status);

        processedEventRepository.save(new ProcessedEvent(
                envelope.eventId(),
                envelope.eventType().name(),
                envelope.aggregateId()
        ));
    }

}