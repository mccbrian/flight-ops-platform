package com.flightops.ingestion.service;

import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.envelope.EventEnvelope;
import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.ingestion.dto.FlightOperationRequest;
import com.flightops.ingestion.producer.FlightOperationProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FlightOperationIngestionService {

    private final FlightOperationProducer producer;

    public void ingest(FlightOperationRequest request) {
        var payload = new FlightOperationEvent(
                request.flightId(),
                request.operationType(),
                request.status(),
                request.gate(),
                request.delayMinutes(),
                request.reason(),
                request.eventTime() == null ? Instant.now() : request.eventTime()
        );

        var envelope = new EventEnvelope<>(
                UUID.randomUUID(),
                EventType.FLIGHT_OPERATION_EVENT,
                String.valueOf(request.flightId()),
                UUID.randomUUID().toString(),
                Instant.now(),
                payload
        );

        producer.publish(envelope);
    }

}