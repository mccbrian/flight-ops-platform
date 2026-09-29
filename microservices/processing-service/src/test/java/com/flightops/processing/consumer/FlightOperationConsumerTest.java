package com.flightops.processing.consumer;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.ProcessingExceptionHandler;
import com.flightops.processing.mapper.FlightOperationMapper;
import com.flightops.processing.service.EventProcessingCoordinator;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Flight Operation Consumer")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationConsumerTest {

    @Mock
    private FlightOperationMapper flightOperationMapper;

    @Mock
    private EventProcessingCoordinator coordinator;

    @Mock
    private ProcessingExceptionHandler exceptionHandler;

    @Mock
    private Acknowledgment acknowledgment;

    private FlightOperationConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new FlightOperationConsumer(flightOperationMapper, coordinator, exceptionHandler);
    }

    @Test
    void shouldProcessEnvelopeAndAcknowledgeSuccessfully() {
        FlightOperationEnvelope envelope = envelope();
        EventEnvelopeJson eventEnvelope = eventEnvelope();

        when(flightOperationMapper.toEventEnvelopeJson(envelope)).thenReturn(eventEnvelope);

        consumer.consume(envelope, acknowledgment);

        verify(flightOperationMapper).toEventEnvelopeJson(envelope);

        verify(coordinator).processEnvelope(eventEnvelope, 1);

        verify(acknowledgment).acknowledge();

        verifyNoInteractions(exceptionHandler);
    }

    @Test
    void shouldNotAcknowledgeWhenProcessingFails() {
        FlightOperationEnvelope envelope = envelope();
        EventEnvelopeJson eventEnvelope = eventEnvelope();

        when(flightOperationMapper.toEventEnvelopeJson(envelope)).thenReturn(eventEnvelope);

        RuntimeException exception = new RuntimeException("Processing failed");

        doThrow(exception).when(coordinator).processEnvelope(eventEnvelope, 1);

        assertThrows(RuntimeException.class, () -> consumer.consume(envelope, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void shouldDelegateProcessingFailureToExceptionHandler() {
        FlightOperationEnvelope envelope = envelope();
        EventEnvelopeJson eventEnvelope = eventEnvelope();

        when(flightOperationMapper.toEventEnvelopeJson(envelope)).thenReturn(eventEnvelope);

        RuntimeException exception = new RuntimeException("Processing failed");

        doThrow(exception).when(coordinator).processEnvelope(eventEnvelope, 1);

        assertThrows(RuntimeException.class, () -> consumer.consume(envelope, acknowledgment));

        verify(exceptionHandler).handleConsumerException(
                "flight-ops.ingestion.v1",
                envelope.getEventId(),
                envelope.getCorrelationId(),
                envelope.getAggregateId(),
                exception
        );
    }

    @Test
    void shouldRethrowProcessingFailure() {
        FlightOperationEnvelope envelope = envelope();
        EventEnvelopeJson eventEnvelope = eventEnvelope();

        when(flightOperationMapper.toEventEnvelopeJson(envelope)).thenReturn(eventEnvelope);

        RuntimeException exception = new RuntimeException("Processing failed");

        doThrow(exception).when(coordinator).processEnvelope(eventEnvelope, 1);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> consumer.consume(envelope, acknowledgment));

        assertSame(exception, thrown);
    }

    private FlightOperationEnvelope envelope() {
        return FlightOperationEnvelope.newBuilder()
                .setEventId("event-123")
                .setEventType("FLIGHT_OPERATION_EVENT")
                .setAggregateId("1001")
                .setCorrelationId("correlation-123")
                .setOccurredAt(Instant.now())
                .setPayload(
                        com.flightops.contracts.avro.FlightOperationEvent.newBuilder()
                                .setFlightId(1001)
                                .setOperationType("DEPARTED")
                                .setStatus("ON_TIME")
                                .setGate("A10")
                                .setDelayMinutes(0)
                                .setReason("Scheduled departure")
                                .setEventTime(Instant.now())
                                .build()
                )
                .build();
    }

    private EventEnvelopeJson eventEnvelope() {
        return new EventEnvelopeJson(
                UUID.randomUUID(),
                null,
                "1001",
                "correlation-123",
                Instant.now(),
                null
        );
    }
}