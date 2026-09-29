package com.flightops.processing.service;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.component.FailureRouter;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.dto.Payload;
import com.flightops.processing.exception.EventParsingException;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.metrics.FlightOperationMetrics;
import com.flightops.processing.utility.CamelCaseFormatter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Event Processing Coordinator")
@DisplayNameGeneration(CamelCaseFormatter.class)
class EventProcessingCoordinatorTest {

    @Mock
    private FlightOperationMetrics metrics;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private EventIdempotencyService idempotencyService;

    @Mock
    private FlightOperationProcessingService processingService;

    @Mock
    private FailureRouter failureRouter;

    @Mock
    private Timer.Sample timerSample;

    private EventProcessingCoordinator coordinator;

    @Nested
    class WhenMicrometerMetricsAreEnabledEventProcessingCoordinator {
        @BeforeEach
        void setUp() {
            coordinator = new EventProcessingCoordinator(
                    metrics,
                    objectMapper,
                    idempotencyService,
                    processingService,
                    failureRouter
            );

            when(metrics.startProcessingTimer()).thenReturn(timerSample);

            // Ensures each test starts with a clean MDC.
            MDC.clear();
        }

        @Test
        void shouldProcessSuccessfullyClaimedEvent() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            coordinator.processEnvelope(envelope, 1);

            verify(idempotencyService).claimForProcessing(envelope.eventId());

            verify(objectMapper).convertValue(envelope.payload(), FlightOperationEvent.class);

            verify(processingService).process(envelope, event);

            verify(idempotencyService).markProcessed(envelope.eventId());

            verify(metrics).incrementProcessed();

            verify(metrics).stopProcessingTimer(timerSample);
        }

        @Test
        void shouldIgnoreDuplicateEvent() {
            EventEnvelopeJson envelope = createEnvelope();

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(false);

            coordinator.processEnvelope(envelope, 1);

            verify(idempotencyService).claimForProcessing(envelope.eventId());

            verify(metrics).incrementDuplicate();

            verify(processingService, never()).process(any(), any());

            verify(idempotencyService, never()).markProcessed(any());

            verify(metrics, never()).incrementProcessed();

            verify(metrics).stopProcessingTimer(timerSample);
        }

        @Test
        void shouldConvertPayloadBeforeProcessing() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            coordinator.processEnvelope(envelope, 1);

            ArgumentCaptor<Payload> payloadCaptor = ArgumentCaptor.forClass(Payload.class);

            verify(objectMapper).convertValue(payloadCaptor.capture(), eq(FlightOperationEvent.class));

            assertEquals(envelope.payload(), payloadCaptor.getValue());

            verify(processingService).process(envelope, event);
        }

        @Test
        void shouldRouteValidationFailure() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            FlightOperationValidationException exception = new FlightOperationValidationException(envelope.eventId(), List.of());

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            org.mockito.Mockito.doThrow(exception).when(processingService).process(envelope, event);

            coordinator.processEnvelope(envelope, 1);

            verify(failureRouter).routeValidationFailure(envelope, 1, exception);

            verify(idempotencyService, never()).markProcessed(envelope.eventId());

            verify(metrics, never()).incrementProcessed();

            verify(metrics).stopProcessingTimer(timerSample);
        }

        @Test
        void shouldRouteUnexpectedFailure() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            RuntimeException exception = new RuntimeException("Unexpected processing failure");

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            org.mockito.Mockito.doThrow(exception).when(processingService).process(envelope, event);

            coordinator.processEnvelope(envelope, 2);

            verify(failureRouter).handleUnexpectedFailure(envelope, 2, exception);

            verify(idempotencyService, never()).markProcessed(envelope.eventId());

            verify(metrics, never()).incrementProcessed();

            verify(metrics).stopProcessingTimer(timerSample);
        }

        @Test
        void shouldCleanUpMdcAfterSuccessfulProcessing() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            coordinator.processEnvelope(envelope, 1);

            assertNull(MDC.get("eventId"));
            assertNull(MDC.get("correlationId"));
            assertNull(MDC.get("aggregateId"));
        }

        @Test
        void shouldCleanUpMdcAfterProcessingFailure() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            RuntimeException exception = new RuntimeException("Unexpected failure");

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            org.mockito.Mockito.doThrow(exception).when(processingService).process(envelope, event);

            coordinator.processEnvelope(envelope, 1);

            assertNull(MDC.get("eventId"));
            assertNull(MDC.get("correlationId"));
            assertNull(MDC.get("aggregateId"));
        }

        @Test
        void shouldStopProcessingTimerWhenProcessingFails() {
            EventEnvelopeJson envelope = createEnvelope();

            RuntimeException exception = new RuntimeException("Unexpected failure");

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenThrow(exception);

            coordinator.processEnvelope(envelope, 1);

            verify(failureRouter).handleUnexpectedFailure(envelope, 1, exception);

            verify(metrics).stopProcessingTimer(timerSample);
        }

        @Test
        void shouldProcessRawEventAfterDeserialization() {
            EventEnvelopeJson envelope = createEnvelope();
            FlightOperationEvent event = createEvent();

            String rawMessage = "{\"eventId\":\"test\"}";

            when(objectMapper.readValue(rawMessage, EventEnvelopeJson.class)).thenReturn(envelope);

            when(idempotencyService.claimForProcessing(envelope.eventId())).thenReturn(true);

            when(objectMapper.convertValue(envelope.payload(), FlightOperationEvent.class)).thenReturn(event);

            coordinator.processRawEvent(rawMessage, 2);

            verify(objectMapper).readValue(rawMessage, EventEnvelopeJson.class);

            verify(processingService).process(envelope, event);
        }
    }

    @Test
    void shouldThrowEventParsingExceptionWhenRawEventCannotBeParsed() {
        coordinator = new EventProcessingCoordinator(
                metrics,
                objectMapper,
                idempotencyService,
                processingService,
                failureRouter
        );

        String rawMessage = "invalid-json";

        RuntimeException parsingFailure = new RuntimeException("Malformed JSON");

        when(objectMapper.readValue(rawMessage, EventEnvelopeJson.class)).thenThrow(parsingFailure);

        EventParsingException exception = org.junit.jupiter.api.Assertions.assertThrows(
                EventParsingException.class,
                () -> coordinator.processRawEvent(rawMessage, 1)
        );

        assertEquals("Failed to parse event envelope", exception.getMessage());

        assertSameCause(parsingFailure, exception);

        verify(processingService, never()).process(any(), any());

        verify(failureRouter, never()).handleUnexpectedFailure(any(), any(int.class), any());

        verify(metrics, never()).startProcessingTimer();
    }

    private EventEnvelopeJson createEnvelope() {
        return new EventEnvelopeJson(
                UUID.randomUUID(),
                EventType.FLIGHT_OPERATION_EVENT,
                "1001",
                UUID.randomUUID().toString(),
                Instant.parse("2026-08-18T12:00:00Z"),
                createPayload()
        );
    }

    private Payload createPayload() {
        return new Payload(
                1001,
                OperationType.DELAY,
                "DELAYED",
                "A12",
                0,
                "WEATHER",
                Instant.parse("2026-08-18T12:30:00Z")
        );
    }

    private FlightOperationEvent createEvent() {
        return FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("DELAY")
                .setStatus("DELAYED")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("WEATHER")
                .setEventTime(Instant.parse("2026-08-18T12:30:00Z"))
                .build();
    }

    private void assertSameCause(Throwable expectedCause, Throwable exception) {
        assertInstanceOf(EventParsingException.class, exception);
        assertEquals(expectedCause, exception.getCause());
    }
}