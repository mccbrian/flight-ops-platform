package com.flightops.processing.service;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.component.FlightOperationStatusUpdater;
import com.flightops.processing.domain.FlightOperationStatus;
import com.flightops.processing.domain.ProcessedEvent;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.dto.Payload;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.repository.FlightOperationStatusRepository;
import com.flightops.processing.repository.ProcessedEventRepository;
import com.flightops.processing.utility.CamelCaseFormatter;
import com.flightops.processing.validation.FlightOperationValidator;
import com.flightops.processing.validation.ValidationError;
import com.flightops.processing.validation.ValidationErrorType;
import com.flightops.processing.validation.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Flight Operation Processing Service")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationProcessingServiceTest {

    @Mock
    private FlightOperationValidator validator;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private FlightOperationStatusRepository statusRepository;

    @Mock
    private FlightOperationStatusUpdater statusUpdater;

    @InjectMocks
    private FlightOperationProcessingService service;

    private EventEnvelopeJson envelope;
    private FlightOperationEvent event;

    @BeforeEach
    void setUp() {
        envelope = createEnvelope();
        event = createEvent();

        when(validator.validate(event))
                .thenReturn(ValidationResult.success());
    }

    @Test
    void shouldThrowAFlightOperationValidationExceptionWhenValidationFails() {
        ValidationError error = new ValidationError(
                ValidationErrorType.NON_RETRYABLE,
                "FLIGHT_NOT_FOUND",
                "No flight exists for flight ID=1001"
        );

        ValidationResult validationResult =
                ValidationResult.failure(List.of(error));

        when(validator.validate(event))
                .thenReturn(validationResult);

        FlightOperationValidationException exception =
                assertThrows(
                        FlightOperationValidationException.class,
                        () -> service.process(envelope, event)
                );

        assertEquals(envelope.eventId(), exception.eventId());

        assertEquals(List.of(error), exception.errors());

        verify(processedEventRepository, never())
                .existsById(any());

        verify(statusRepository, never())
                .findById(any());

        verify(statusRepository, never())
                .save(any());

        verify(processedEventRepository, never())
                .save(any());
    }

    @Test
    void shouldReturnWithoutProcessingWhenTheEventHasAlreadyBeenProcessed() {
        when(processedEventRepository.existsById(envelope.eventId())).thenReturn(true);

        service.process(envelope, event);

        verify(processedEventRepository)
                .existsById(envelope.eventId());

        verify(statusRepository, never())
                .findById(any());

        verify(statusRepository, never())
                .save(any());

        verify(statusUpdater, never())
                .apply(any(), any());

        verify(processedEventRepository, never())
                .save(any());
    }

    @Test
    void shouldCreateAndSaveAFlightOperationStatusWhenNoExistingStatusIsFound() {
        when(statusRepository.findById(event.getFlightId())).thenReturn(Optional.empty());

        FlightOperationStatus status = FlightOperationStatus.initialize(event.getFlightId());

        when(statusUpdater.apply(any(), any())).thenReturn(true);

        when(statusRepository.save(any())).thenReturn(status);

        service.process(envelope, event);

        ArgumentCaptor<FlightOperationStatus> statusCaptor = ArgumentCaptor.forClass(FlightOperationStatus.class);

        verify(statusRepository)
                .save(statusCaptor.capture());

        assertEquals(event.getFlightId(), statusCaptor.getValue().getFlightId());

        verify(statusUpdater)
                .apply(statusCaptor.getValue(), event);

        verify(processedEventRepository)
                .save(any(ProcessedEvent.class));
    }

    @Test
    void shouldUpdateAndSaveAnExistingFlightOperationStatusWhenTheEventIsApplied() {
        FlightOperationStatus status = FlightOperationStatus.initialize(event.getFlightId());

        status.setLastEventTime(Instant.parse("2026-08-18T12:00:00Z"));

        when(statusRepository.findById(event.getFlightId())).thenReturn(Optional.of(status));

        when(statusUpdater.apply(status, event)).thenReturn(true);

        service.process(envelope, event);

        verify(statusUpdater)
                .apply(status, event);

        verify(statusRepository)
                .save(status);

        verify(processedEventRepository)
                .save(any(ProcessedEvent.class));
    }

    @Test
    void shouldNotSaveTheFlightOperationStatusWhenTheEventIsNotApplied() {
        FlightOperationStatus status = FlightOperationStatus.initialize(event.getFlightId());

        status.setLastEventTime(Instant.parse("2026-08-18T13:00:00Z"));

        when(statusRepository.findById(event.getFlightId())).thenReturn(Optional.of(status));

        when(statusUpdater.apply(status, event)).thenReturn(false);

        service.process(envelope, event);

        verify(statusUpdater)
                .apply(status, event);

        verify(statusRepository, never())
                .save(any());

        verify(processedEventRepository)
                .save(any(ProcessedEvent.class));
    }

    @Test
    void shouldSaveTheProcessedEventAfterSuccessfullyProcessingTheEvent() {
        FlightOperationStatus status = FlightOperationStatus.initialize(event.getFlightId());

        when(statusRepository.findById(event.getFlightId())).thenReturn(Optional.of(status));

        when(statusUpdater.apply(status, event)).thenReturn(true);

        ArgumentCaptor<ProcessedEvent> processedEventCaptor = ArgumentCaptor.forClass(ProcessedEvent.class);

        service.process(envelope, event);

        verify(processedEventRepository)
                .save(processedEventCaptor.capture());

        ProcessedEvent processedEvent = processedEventCaptor.getValue();

        assertEquals(envelope.eventId(), processedEvent.getEventId());

        assertEquals(envelope.eventType().name(), processedEvent.getEventType());

        assertEquals(envelope.aggregateId(), processedEvent.getAggregateId());
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

    private FlightOperationEvent createEvent() {
        return FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("ARRIVAL")
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(Instant.parse("2026-08-18T12:30:00Z"))
                .build();
    }

    private Payload createPayload() {
        return new Payload(
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