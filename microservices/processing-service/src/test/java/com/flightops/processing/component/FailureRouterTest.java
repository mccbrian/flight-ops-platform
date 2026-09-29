package com.flightops.processing.component;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.exception.FlightOperationValidationException;
import com.flightops.processing.idempotency.EventIdempotencyService;
import com.flightops.processing.metrics.FlightOperationMetrics;
import com.flightops.processing.producer.FailureEventProducer;
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

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Failure Router")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FailureRouterTest {

    private static final int MAX_ATTEMPTS = 3;

    @Mock
    private FlightOperationMetrics metrics;

    @Mock
    private FailedEventFactory failedEventFactory;

    @Mock
    private FailureEventProducer failureEventProducer;

    @Mock
    private EventIdempotencyService idempotencyService;

    private FailureRouter failureRouter;

    @BeforeEach
    void setUp() {
        failureRouter = new FailureRouter(
                metrics,
                failedEventFactory,
                failureEventProducer,
                idempotencyService
        );

        ReflectionTestUtils.setField(
                failureRouter,
                "maxAttempts",
                MAX_ATTEMPTS
        );
    }

    @Test
    void shouldRouteRetryableValidationFailureToRetryTopic() {
        EventEnvelopeJson envelope = envelope();
        FailedEvent failedEvent = failedEvent();
        ValidationError error = retryableError();

        FlightOperationValidationException exception =
                validationException(envelope, error);

        when(failedEventFactory.buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(List.of(error)),
                any(String.class),
                eq(1)
        )).thenReturn(failedEvent);

        failureRouter.routeValidationFailure(envelope, 1, exception);

        verify(failedEventFactory).buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(List.of(error)),
                any(String.class),
                eq(1)
        );

        verify(failureEventProducer).sendToRetry(failedEvent);
        verify(metrics).incrementRetry();

        verify(failureEventProducer, never()).sendToDlq(failedEvent);
        verify(metrics, never()).incrementDlq();
    }

    @Test
    void shouldRouteRetryableValidationFailureToDlqWhenMaxAttemptsReached() {
        EventEnvelopeJson envelope = envelope();
        FailedEvent failedEvent = failedEvent();
        ValidationError error = retryableError();

        FlightOperationValidationException exception =
                validationException(envelope, error);

        when(failedEventFactory.buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(List.of(error)),
                any(String.class),
                eq(MAX_ATTEMPTS)
        )).thenReturn(failedEvent);

        failureRouter.routeValidationFailure(
                envelope,
                MAX_ATTEMPTS,
                exception
        );
        
        verify(failureEventProducer).sendToDlq(failedEvent);
        verify(metrics).incrementDlq();
        verify(metrics).incrementFailed();

        verify(failureEventProducer, never()).sendToRetry(failedEvent);
        verify(metrics, never()).incrementRetry();
    }

    @Test
    void shouldRouteNonRetryableValidationFailureToDlq() {
        EventEnvelopeJson envelope = envelope();
        FailedEvent failedEvent = failedEvent();
        ValidationError error = nonRetryableError();

        FlightOperationValidationException exception =
                validationException(envelope, error);

        when(failedEventFactory.buildFailedEvent(
                eq(envelope),
                eq(false),
                eq(List.of(error)),
                any(String.class),
                eq(1)
        )).thenReturn(failedEvent);

        failureRouter.routeValidationFailure(
                envelope,
                1,
                exception
        );

        verify(failureEventProducer).sendToDlq(failedEvent);
        verify(metrics).incrementDlq();
        verify(metrics).incrementFailed();

        verify(failureEventProducer, never()).sendToRetry(failedEvent);
        verify(metrics, never()).incrementRetry();
    }

    @Test
    void shouldRouteValidationFailureToRetryWhenAnyErrorIsRetryable() {
        EventEnvelopeJson envelope = envelope();
        FailedEvent failedEvent = failedEvent();

        ValidationError nonRetryable = nonRetryableError();
        ValidationError retryable = retryableError();

        List<ValidationError> errors = List.of(
                nonRetryable,
                retryable
        );

        FlightOperationValidationException exception =
                validationException(envelope, nonRetryable, retryable);

        when(failedEventFactory.buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(errors),
                any(String.class),
                eq(1)
        )).thenReturn(failedEvent);

        failureRouter.routeValidationFailure(
                envelope,
                1,
                exception
        );

        verify(failedEventFactory).buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(errors),
                any(String.class),
                eq(1)
        );

        verify(failureEventProducer).sendToRetry(failedEvent);
        verify(metrics).incrementRetry();

        verify(failureEventProducer, never()).sendToDlq(failedEvent);
    }

    @Test
    void shouldHandleNullEnvelope() {
        ValidationError error = nonRetryableError();

        FlightOperationValidationException exception =
                validationException(null, error);

        assertDoesNotThrow(() ->
                failureRouter.routeValidationFailure(
                        null,
                        1,
                        exception
                )
        );

        verify(metrics).incrementFailed();

        verify(failedEventFactory, never()).buildFailedEvent(
                any(),
                any(boolean.class),
                any(),
                any(),
                any(int.class)
        );

        verify(failureEventProducer, never()).sendToRetry(any());
        verify(failureEventProducer, never()).sendToDlq(any());
        verify(idempotencyService, never()).releaseClaim(any());
    }

    @Test
    void shouldRouteUnexpectedFailureToRetry() {
        EventEnvelopeJson envelope = envelope();
        FailedEvent failedEvent = failedEvent();
        RuntimeException exception = new RuntimeException("Unexpected failure");

        when(failedEventFactory.buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(List.of()),
                any(String.class),
                eq(1)
        )).thenReturn(failedEvent);

        failureRouter.handleUnexpectedFailure(
                envelope,
                1,
                exception
        );

        verify(failedEventFactory).buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(List.of()),
                any(String.class),
                eq(1)
        );

        verify(failureEventProducer).sendToRetry(failedEvent);
        verify(metrics).incrementRetry();

        verify(failureEventProducer, never()).sendToDlq(failedEvent);
    }

    @Test
    void shouldRouteUnexpectedFailureToDlqWhenMaxAttemptsReached() {
        EventEnvelopeJson envelope = envelope();
        FailedEvent failedEvent = failedEvent();
        RuntimeException exception = new RuntimeException("Unexpected failure");

        when(failedEventFactory.buildFailedEvent(
                eq(envelope),
                eq(true),
                eq(List.of()),
                any(String.class),
                eq(MAX_ATTEMPTS)
        )).thenReturn(failedEvent);

        failureRouter.handleUnexpectedFailure(
                envelope,
                MAX_ATTEMPTS,
                exception
        );

        verify(failureEventProducer).sendToDlq(failedEvent);
        verify(metrics).incrementDlq();
        verify(metrics).incrementFailed();

        verify(failureEventProducer, never()).sendToRetry(failedEvent);
        verify(metrics, never()).incrementRetry();
    }

    private FlightOperationValidationException validationException(
            EventEnvelopeJson envelope,
            ValidationError... errors
    ) {
        return new FlightOperationValidationException(
                envelope == null ? UUID.randomUUID() : envelope.eventId(),
                List.of(errors)
        );
    }

    private ValidationError retryableError() {
        return new ValidationError(
                ValidationErrorType.RETRYABLE,
                "Temporary processing failure",
                "No flight exists for flight ID="
        );
    }

    private ValidationError nonRetryableError() {
        return new ValidationError(
                ValidationErrorType.NON_RETRYABLE,
                "FLIGHT_NOT_FOUND",
                "No flight exists for flight ID="
        );
    }

    private EventEnvelopeJson envelope() {
        return new EventEnvelopeJson(
                UUID.randomUUID(),
                EventType.FLIGHT_OPERATION_EVENT,
                "1001",
                UUID.randomUUID().toString(),
                java.time.Instant.now(),
                null
        );
    }

    private FailedEvent failedEvent() {
        return FailedEvent.newBuilder()
                .setOriginalEventId(UUID.randomUUID().toString())
                .setOriginalEventType("FLIGHT_OPERATION")
                .setAggregateId("1001")
                .setCorrelationId(UUID.randomUUID().toString())
                .setFailureType("RETRYABLE")
                .setErrorCodes(List.of("TEMPORARY_FAILURE"))
                .setReason("Temporary processing failure")
                .setRawPayload("{}")
                .setAttemptCount(1)
                .setMaxAttempts(MAX_ATTEMPTS)
                .setFailedAt(java.time.Instant.now())
                .build();
    }
}

