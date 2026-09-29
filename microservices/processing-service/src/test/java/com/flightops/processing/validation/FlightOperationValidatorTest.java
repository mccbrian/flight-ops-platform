package com.flightops.processing.validation;

import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.repository.FlightReferenceRepository;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Flight Operation Validator")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationValidatorTest {

    @Mock
    private FlightReferenceRepository flightReferenceRepository;

    @InjectMocks
    private FlightOperationValidator validator;

    @BeforeEach
    void setUp() {
        when(flightReferenceRepository.existsByFlightId(1001))
                .thenReturn(true);
    }

    @Test
    void shouldReturnASuccessfulValidationResultWhenTheFlightOperationEventIsValid() {
        FlightOperationEvent event = createValidEvent();

        ValidationResult result = validator.validate(event);

        assertTrue(result.valid());
        assertFalse(result.hasErrors());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    void shouldReturnAFlightNotFoundErrorWhenTheFlightDoesNotExist() {
        when(flightReferenceRepository.existsByFlightId(1001))
                .thenReturn(false);

        FlightOperationEvent event = createValidEvent();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertEquals(1, result.errors().size());

        ValidationError error = result.errors().getFirst();

        assertEquals(
                ValidationErrorType.NON_RETRYABLE,
                error.type()
        );
        assertEquals(
                "FLIGHT_NOT_FOUND",
                error.code()
        );
        assertEquals(
                "No flight exists for flight ID=1001",
                error.message()
        );
    }

    @Test
    void shouldReturnAnUnknownOperationTypeErrorWhenTheOperationTypeIsInvalid() {
        FlightOperationEvent event = createValidEvent(
                "INVALID_OPERATION"
        );

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertEquals(1, result.errors().size());

        ValidationError error = result.errors().getFirst();

        assertEquals(
                ValidationErrorType.NON_RETRYABLE,
                error.type()
        );
        assertEquals(
                "UNKNOWN_OPERATION_TYPE",
                error.code()
        );
        assertEquals(
                "Unsupported operation type=INVALID_OPERATION",
                error.message()
        );
    }

    @Test
    void shouldReturnAStatusRequiredErrorWhenTheStatusIsBlank() {
        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(OperationType.ARRIVAL.name())
                .setStatus("")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(Instant.now())
                .build();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());

        assertTrue(
                result.errors().stream()
                        .anyMatch(error ->
                                error.code().equals("STATUS_REQUIRED")
                        )
        );
    }

    @Test
    void shouldReturnAGateRequiredErrorWhenTheGateIsBlank() {
        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(OperationType.ARRIVAL.name())
                .setStatus("ON_TIME")
                .setGate("")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(Instant.now())
                .build();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());

        assertTrue(
                result.errors().stream()
                        .anyMatch(error ->
                                error.code().equals("GATE_REQUIRED")
                        )
        );
    }

    @Test
    void shouldReturnAnInvalidDelayMinutesErrorWhenDelayMinutesIsNegative() {
        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(OperationType.ARRIVAL.name())
                .setStatus("DELAYED")
                .setGate("A12")
                .setDelayMinutes(-1)
                .setReason("Weather")
                .setEventTime(Instant.now())
                .build();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());

        assertTrue(
                result.errors().stream()
                        .anyMatch(error ->
                                error.code().equals("INVALID_DELAY_MINUTES")
                        )
        );
    }

    @Test
    void shouldReturnAReasonRequiredErrorWhenTheReasonIsBlank() {
        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(OperationType.ARRIVAL.name())
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("")
                .setEventTime(Instant.now())
                .build();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());

        assertTrue(
                result.errors().stream()
                        .anyMatch(error ->
                                error.code().equals("REASON_REQUIRED")
                        )
        );
    }

    @Test
    void shouldReturnAClockSkewErrorWhenTheEventTimeIsTooFarInTheFuture() {
        Instant futureEventTime =
                Instant.now().plusSeconds(11);

        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(OperationType.ARRIVAL.name())
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(futureEventTime)
                .build();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());

        ValidationError error = result.errors().stream()
                .filter(validationError ->
                        validationError.code()
                                .equals("EVENT_TIME_CLOCK_SKEW_TOO_LARGE")
                )
                .findFirst()
                .orElseThrow();

        assertEquals(
                ValidationErrorType.NON_RETRYABLE,
                error.type()
        );

        assertEquals(
                "EVENT_TIME_CLOCK_SKEW_TOO_LARGE",
                error.code()
        );
    }

    @Test
    void shouldReturnMultipleValidationErrorsWhenMultipleRulesAreViolated() {
        when(flightReferenceRepository.existsByFlightId(1001))
                .thenReturn(false);

        FlightOperationEvent event = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("INVALID_OPERATION")
                .setStatus("")
                .setGate("")
                .setDelayMinutes(-1)
                .setReason("")
                .setEventTime(Instant.now().plusSeconds(11))
                .build();

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertEquals(7, result.errors().size());

        List<String> errorCodes = result.errors().stream()
                .map(ValidationError::code)
                .toList();

        assertTrue(errorCodes.contains("FLIGHT_NOT_FOUND"));
        assertTrue(errorCodes.contains("UNKNOWN_OPERATION_TYPE"));
        assertTrue(errorCodes.contains("STATUS_REQUIRED"));
        assertTrue(errorCodes.contains("GATE_REQUIRED"));
        assertTrue(errorCodes.contains("INVALID_DELAY_MINUTES"));
        assertTrue(errorCodes.contains("REASON_REQUIRED"));
        assertTrue(errorCodes.contains("EVENT_TIME_CLOCK_SKEW_TOO_LARGE"));
    }

    private FlightOperationEvent createValidEvent() {
        return createValidEvent(OperationType.ARRIVAL.name());
    }

    private FlightOperationEvent createValidEvent(String operationType) {
        return FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType(operationType)
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(Instant.now())
                .build();
    }
}
