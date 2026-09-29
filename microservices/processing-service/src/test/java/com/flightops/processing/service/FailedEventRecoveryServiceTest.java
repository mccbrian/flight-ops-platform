package com.flightops.processing.service;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("Failed Event Recovery Service")
@DisplayNameGeneration(CamelCaseFormatter.class)
class FailedEventRecoveryServiceTest {

    @Mock
    private EventProcessingCoordinator coordinator;

    private FailedEventRecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        recoveryService = new FailedEventRecoveryService(coordinator);
    }

    @Test
    void shouldRecoverFailedEventWithIncrementedAttemptCount() {
        FailedEvent failedEvent = failedEvent(1);

        recoveryService.recover(failedEvent);

        verify(coordinator).processRawEvent(failedEvent.getRawPayload(), 2);
    }

    @Test
    void shouldPassRawPayloadToCoordinator() {
        FailedEvent failedEvent = failedEvent(2);

        recoveryService.recover(failedEvent);

        verify(coordinator).processRawEvent(failedEvent.getRawPayload(), 3);
    }

    @Test
    void shouldPropagateCoordinatorException() {
        FailedEvent failedEvent = failedEvent(1);

        RuntimeException exception = new RuntimeException("Processing failed");

        doThrow(exception).when(coordinator).processRawEvent(failedEvent.getRawPayload(), 2);

        RuntimeException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class,
                () -> recoveryService.recover(failedEvent)
        );

        org.junit.jupiter.api.Assertions.assertSame(exception, thrown);
    }

    private FailedEvent failedEvent(int attemptCount) {
        return FailedEvent.newBuilder()
                .setOriginalEventId(UUID.randomUUID().toString())
                .setOriginalEventType("FLIGHT_OPERATION_EVENT")
                .setAggregateId("1001")
                .setCorrelationId(UUID.randomUUID().toString())
                .setFailureType("RETRYABLE")
                .setErrorCodes(List.of("TEMPORARY_FAILURE"))
                .setReason("Temporary processing failure")
                .setRawPayload("{\"eventId\":\"test-event\"}")
                .setAttemptCount(attemptCount)
                .setMaxAttempts(3)
                .setFailedAt(Instant.now())
                .build();
    }
}