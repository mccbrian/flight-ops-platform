package com.flightops.processing.component;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.dto.Payload;
import com.flightops.processing.utility.CamelCaseFormatter;
import com.flightops.processing.validation.ValidationError;
import com.flightops.processing.validation.ValidationErrorType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.flightops.contracts.enums.OperationType.ARRIVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Failed Event Factory")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FailedEventFactoryTest {

    @Mock
    private ObjectMapper objectMapper;

    private FailedEventFactory factory;

    private final int maxAttempts = 3;

    @BeforeEach
    void setUp() {
        factory = new FailedEventFactory(objectMapper);

        ReflectionTestUtils.setField(
                factory,
                "maxAttempts",
                maxAttempts
        );
    }

    @Test
    void shouldBuildARetryableFailedEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();

        EventEnvelopeJson envelope = createEnvelope(
                eventId,
                correlationId
        );

        List<ValidationError> errors = List.of(
                new ValidationError(
                        ValidationErrorType.RETRYABLE,
                        "TEMPORARY_FAILURE",
                        "Temporary processing failure"
                ),
                new ValidationError(
                        ValidationErrorType.RETRYABLE,
                        "DEPENDENCY_UNAVAILABLE",
                        "Dependency unavailable"
                )
        );

        String rawPayload = "{\"eventId\":\"" + eventId + "\"}";

        when(objectMapper.writeValueAsString(envelope)).thenReturn(rawPayload);

        Instant before = Instant.now();

        FailedEvent result = factory.buildFailedEvent(
                envelope,
                true,
                errors,
                "Temporary processing failure",
                2
        );

        Instant after = Instant.now();

        assertEquals(eventId.toString(), result.getOriginalEventId());

        assertEquals(EventType.FLIGHT_OPERATION_EVENT.name(), result.getOriginalEventType());

        assertEquals(envelope.aggregateId(), result.getAggregateId());

        assertEquals(correlationId, result.getCorrelationId());

        assertEquals("RETRYABLE", result.getFailureType());

        assertEquals(List.of("TEMPORARY_FAILURE", "DEPENDENCY_UNAVAILABLE"), result.getErrorCodes());

        assertEquals("Temporary processing failure", result.getReason());

        assertEquals(rawPayload, result.getRawPayload());

        assertEquals(2, result.getAttemptCount());

        assertEquals(maxAttempts, result.getMaxAttempts());

        assertNotNull(result.getFailedAt());

        assertNotNull(result.getFailedAt());
    }

    @Test
    void shouldBuildANonRetryableFailedEvent() {
        EventEnvelopeJson envelope = createEnvelope(UUID.randomUUID(), UUID.randomUUID().toString());

        List<ValidationError> errors = List.of(
                new ValidationError(
                        ValidationErrorType.NON_RETRYABLE,
                        "INVALID_EVENT",
                        "Event is invalid"
                )
        );

        String rawPayload = "{\"event\":\"invalid\"}";

        when(objectMapper.writeValueAsString(envelope)).thenReturn(rawPayload);

        FailedEvent result = factory.buildFailedEvent(
                envelope,
                false,
                errors,
                "Event is invalid",
                1
        );

        assertEquals("NON_RETRYABLE", result.getFailureType());

        assertEquals(List.of("INVALID_EVENT"), result.getErrorCodes());

        assertEquals("Event is invalid", result.getReason());

        assertEquals(rawPayload, result.getRawPayload());

        assertEquals(1, result.getAttemptCount());

        assertEquals(maxAttempts, result.getMaxAttempts());
    }

    @Test
    void shouldBuildAFailedEventWithMultipleErrorCodes() throws Exception {
        EventEnvelopeJson envelope = createEnvelope(UUID.randomUUID(), UUID.randomUUID().toString());

        List<ValidationError> errors = List.of(
                new ValidationError(
                        ValidationErrorType.NON_RETRYABLE,
                        "INVALID_GATE",
                        "Invalid gate"
                ),
                new ValidationError(
                        ValidationErrorType.NON_RETRYABLE,
                        "INVALID_STATUS",
                        "Invalid status"
                ),
                new ValidationError(
                        ValidationErrorType.NON_RETRYABLE,
                        "INVALID_DELAY",
                        "Invalid delay"
                )
        );

        when(objectMapper.writeValueAsString(envelope)).thenReturn("{}");

        FailedEvent result = factory.buildFailedEvent(
                envelope,
                false,
                errors,
                "Multiple validation failures",
                1
        );

        assertEquals(
                List.of(
                        "INVALID_GATE",
                        "INVALID_STATUS",
                        "INVALID_DELAY"
                ),
                result.getErrorCodes()
        );
    }

    @Test
    void shouldBuildAFailedEventWithNoErrorCodesWhenTheErrorListIsEmpty()
            throws Exception {

        EventEnvelopeJson envelope = createEnvelope(UUID.randomUUID(), UUID.randomUUID().toString());

        when(objectMapper.writeValueAsString(envelope)).thenReturn("{}");

        FailedEvent result = factory.buildFailedEvent(
                envelope,
                false,
                List.of(),
                "Processing failed",
                3
        );

        assertTrue(result.getErrorCodes().isEmpty());

        assertEquals("Processing failed", result.getReason());
    }

    @Test
    void shouldThrowAnIllegalStateExceptionWhenTheEnvelopeCannotBeSerialized() throws Exception {

        EventEnvelopeJson envelope = createEnvelope(UUID.randomUUID(), UUID.randomUUID().toString());

        when(objectMapper.writeValueAsString(envelope)).thenThrow(new RuntimeException("Serialization failed"));

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> factory.buildFailedEvent(
                                envelope,
                                false,
                                List.of(),
                                "Processing failed",
                                1
                        )
                );

        assertEquals("Failed to serialize original event envelope", exception.getMessage());

        assertNotNull(exception.getCause());

        assertEquals("Serialization failed", exception.getCause().getMessage());
    }

    private EventEnvelopeJson createEnvelope(UUID eventId, String correlationId) {

        Payload payload = new Payload(
                1001,
                ARRIVAL,
                "ON_TIME",
                "A12",
                0,
                "Scheduled",
                Instant.parse("2026-08-18T12:30:00Z")
        );

        return new EventEnvelopeJson(
                eventId,
                EventType.FLIGHT_OPERATION_EVENT,
                "1001",
                correlationId,
                Instant.parse("2026-08-18T12:00:00Z"),
                payload
        );
    }
}