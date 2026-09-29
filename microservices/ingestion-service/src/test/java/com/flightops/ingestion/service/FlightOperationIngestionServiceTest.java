package com.flightops.ingestion.service;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.OperationType;
import com.flightops.ingestion.dto.FlightOperationRequest;
import com.flightops.ingestion.exception.EventPublishException;
import com.flightops.ingestion.producer.FlightOperationProducer;
import com.flightops.ingestion.utility.CamelCaseFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(CamelCaseFormatter.class)
@SuppressWarnings("unchecked")
@DisplayName("Flight Operation Ingestion Service")
class FlightOperationIngestionServiceTest {

    @Mock
    private FlightOperationProducer producer;

    @InjectMocks
    private FlightOperationIngestionService service;

    @Captor
    private ArgumentCaptor<FlightOperationEnvelope> envelopeCaptor;

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void shouldBuildAFlightOperationEnvelopeFromTheRequest() {
        FlightOperationRequest request = createRequest();
        FlightOperationEnvelope envelope = ingestAndCaptureEnvelope();

        assertEquals("FLIGHT_OPERATION_EVENT", envelope.getEventType());
        assertEquals(String.valueOf(request.flightId()), envelope.getAggregateId());

        FlightOperationEvent payload = envelope.getPayload();

        assertEquals(request.flightId(), payload.getFlightId());
        assertEquals(request.operationType().name(), payload.getOperationType());
        assertEquals(request.status(), payload.getStatus());
        assertEquals(request.gate(), payload.getGate());
        assertEquals(request.delayMinutes(), payload.getDelayMinutes());
        assertEquals(request.reason(), payload.getReason());
        assertEquals(request.eventTime(), payload.getEventTime());
    }

    @Test
    void shouldGenerateAnEventIdWhenBuildingTheEnvelope() {
        FlightOperationEnvelope envelope = ingestAndCaptureEnvelope();

        assertNotNull(envelope.getEventId());
        assertFalse(envelope.getEventId().isBlank());
    }

    @Test
    void shouldUseTheCurrentCorrelationIdFromTheMdc() {
        MDC.put("correlationId", "abc-123");

        FlightOperationEnvelope envelope = ingestAndCaptureEnvelope();

        assertEquals("abc-123", envelope.getCorrelationId());
    }

    @Test
    void shouldGenerateACorrelationIdWhenTheMdcDoesNotContainOne() {
        FlightOperationEnvelope envelope = ingestAndCaptureEnvelope();

        assertNotNull(envelope.getCorrelationId());
        assertFalse(envelope.getCorrelationId().isBlank());
    }

    @Test
    void shouldThrowAnEventPublishExceptionWhenPublishingFails() {
        FlightOperationRequest request = createRequest();

        when(producer.publish(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("Kafka unavailable")));

        CompletionException exception = assertThrows(CompletionException.class, () -> service.ingest(request).join());

        assertInstanceOf(EventPublishException.class, exception.getCause());

        assertTrue(exception.getMessage().contains("Failed to publish flight operation"));
    }

    private FlightOperationEnvelope ingestAndCaptureEnvelope() {
        when(producer.publish(any())).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        service.ingest(createRequest()).join();

        verify(producer).publish(envelopeCaptor.capture());

        return envelopeCaptor.getValue();
    }

    private FlightOperationRequest createRequest() {
        return new FlightOperationRequest(
                1001,
                OperationType.DELAY,
                "DELAYED",
                "A12",
                0,
                "WEATHER",
                Instant.parse("2026-08-02T18:30:00Z")
        );
    }

}