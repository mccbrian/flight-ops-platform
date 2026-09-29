package com.flightops.processing.consumer;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.exception.ProcessingExceptionHandler;
import com.flightops.processing.service.FailedEventRecoveryService;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
@DisplayName("Flight Operation Retry Consumer")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FlightOperationRetryConsumerTest {

    @Mock
    private FailedEventRecoveryService recoveryService;

    @Mock
    private ProcessingExceptionHandler exceptionHandler;

    @Mock
    private Acknowledgment acknowledgment;

    private FlightOperationRetryConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new FlightOperationRetryConsumer(recoveryService, exceptionHandler);
    }

    @Test
    void shouldRecoverFailedEventAndAcknowledgeSuccessfully() {
        FailedEvent failedEvent = failedEvent();

        consumer.consumeRetry(failedEvent, acknowledgment);

        verify(recoveryService).recover(failedEvent);
        verify(acknowledgment).acknowledge();

        verifyNoInteractions(exceptionHandler);
    }

    @Test
    void shouldNotAcknowledgeWhenRecoveryFails() {
        FailedEvent failedEvent = failedEvent();
        RuntimeException exception = new RuntimeException("Recovery failed");

        doThrow(exception).when(recoveryService).recover(failedEvent);

        assertThrows(RuntimeException.class, () -> consumer.consumeRetry(failedEvent, acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void shouldDelegateRecoveryFailureToExceptionHandler() {
        FailedEvent failedEvent = failedEvent();
        RuntimeException exception = new RuntimeException("Recovery failed");

        doThrow(exception).when(recoveryService).recover(failedEvent);

        assertThrows(RuntimeException.class, () -> consumer.consumeRetry(failedEvent, acknowledgment));

        verify(exceptionHandler).handleConsumerException(
                "flight-ops.ingestion.retry.v1",
                "event-123",
                "correlation-123",
                "1001",
                exception
        );
    }

    @Test
    void shouldRethrowRecoveryFailure() {
        FailedEvent failedEvent = failedEvent();
        RuntimeException exception = new RuntimeException("Recovery failed");

        doThrow(exception).when(recoveryService).recover(failedEvent);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> consumer.consumeRetry(failedEvent, acknowledgment));

        assertSame(exception, thrown);
    }

    private FailedEvent failedEvent() {
        return FailedEvent.newBuilder()
                .setOriginalEventId("event-123")
                .setOriginalEventType("FLIGHT_OPERATION_EVENT")
                .setAggregateId("1001")
                .setCorrelationId("correlation-123")
                .setFailureType("RETRYABLE")
                .setErrorCodes(List.of("SOME_ERROR"))
                .setReason("Temporary processing failure")
                .setRawPayload("{}")
                .setAttemptCount(1)
                .setMaxAttempts(3)
                .setFailedAt(Instant.now())
                .build();
    }
}