package com.flightops.processing.service;

import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.processing.domain.ProcessedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.repository.FlightOperationStatusRepository;
import com.flightops.processing.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class FlightOperationProcessingService {

    private static final Logger log = LoggerFactory.getLogger(FlightOperationProcessingService.class);

    private final ProcessedEventRepository processedEventRepository;
    private final FlightOperationStatusRepository flightOperationStatusRepository;

    @Transactional
    public void process(EventEnvelopeJson envelope, FlightOperationEvent payload) {

        // DB idempotency
        if (processedEventRepository.existsById(envelope.eventId())) {
            log.info("Duplicate event detected at DB level. eventId={}", envelope.eventId());
            return;
        }

        processedEventRepository.save(
                new ProcessedEvent(
                        envelope.eventId(),
                        envelope.eventType().name(),
                        envelope.aggregateId()
                )
        );

        // Aggregate update
        var existing = flightOperationStatusRepository.findById(payload.flightId());

        if (existing.isPresent()) {
            var entity = existing.get();
            entity.update(payload.status(), payload.gate(), payload.delayMinutes());
            flightOperationStatusRepository.save(entity);
        } else {
            flightOperationStatusRepository.save(
                    new FlightOperationStatus(
                            payload.flightId(),
                            payload.status(),
                            payload.gate(),
                            payload.delayMinutes()
                    )
            );
        }

        log.info("Flight operation persisted. flightId={}", payload.flightId());
    }

}