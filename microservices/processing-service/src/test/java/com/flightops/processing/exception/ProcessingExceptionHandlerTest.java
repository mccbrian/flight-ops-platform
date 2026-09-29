package com.flightops.processing.exception;

import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
@DisplayName("Processing Exception Handler")
@DisplayNameGeneration(CamelCaseFormatter.class)
class ProcessingExceptionHandlerTest {

    private ProcessingExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new ProcessingExceptionHandler();
    }

    @Test
    void shouldHandleConsumerExceptionWithoutThrowing() {
        RuntimeException exception = new RuntimeException("Processing failed");

        assertDoesNotThrow(() ->
                handler.handleConsumerException(
                        "flight-ops.ingestion.v1",
                        "event-123",
                        "correlation-123",
                        "1001",
                        exception
                )
        );
    }
}