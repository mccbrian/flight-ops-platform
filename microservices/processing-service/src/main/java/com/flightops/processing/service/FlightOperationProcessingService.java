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

@Slf4j
@Service
@RequiredArgsConstructor
public class FlightOperationProcessingService {

    private final FlightOperationValidator validator;
    private final ProcessedEventRepository processedEventRepository;
    private final FlightOperationStatusRepository statusRepository;

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