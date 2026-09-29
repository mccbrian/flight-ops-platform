package com.flightops.processing.idempotency;

import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Event Idempotency Service")
@DisplayNameGeneration(CamelCaseFormatter.class)
class EventIdempotencyServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private EventIdempotencyService service;

    @BeforeEach
    void setUp() {
        service = new EventIdempotencyService(redisTemplate);
    }

    @Test
    void shouldReturnFalseWhenTheEventHasAlreadyBeenProcessed() {
        UUID eventId = UUID.randomUUID();

        String processedKey = "processed:event:" + eventId;

        when(redisTemplate.hasKey(processedKey))
                .thenReturn(true);

        boolean result = service.claimForProcessing(eventId);

        assertFalse(result);

        verify(redisTemplate).hasKey(processedKey);

        verify(valueOperations, never())
                .setIfAbsent(
                        "processing:event:" + eventId,
                        "PROCESSING",
                        Duration.ofMinutes(5)
                );
    }

    @Test
    void shouldReturnTrueWhenTheEventIsSuccessfullyClaimedForProcessing() {
        UUID eventId = UUID.randomUUID();

        String processedKey = "processed:event:" + eventId;
        String processingKey = "processing:event:" + eventId;

        when(redisTemplate.hasKey(processedKey))
                .thenReturn(false);

        when(redisTemplate.opsForValue())
                .thenReturn(valueOperations);

        when(valueOperations.setIfAbsent(
                processingKey,
                "PROCESSING",
                Duration.ofMinutes(5)
        )).thenReturn(true);

        boolean result = service.claimForProcessing(eventId);

        assertTrue(result);

        verify(redisTemplate).hasKey(processedKey);

        verify(valueOperations).setIfAbsent(
                processingKey,
                "PROCESSING",
                Duration.ofMinutes(5)
        );
    }

    @Test
    void shouldReturnFalseWhenTheEventIsAlreadyClaimedForProcessing() {
        UUID eventId = UUID.randomUUID();

        String processedKey = "processed:event:" + eventId;
        String processingKey = "processing:event:" + eventId;

        when(redisTemplate.hasKey(processedKey))
                .thenReturn(false);

        when(redisTemplate.opsForValue())
                .thenReturn(valueOperations);

        when(valueOperations.setIfAbsent(
                processingKey,
                "PROCESSING",
                Duration.ofMinutes(5)
        )).thenReturn(false);

        boolean result = service.claimForProcessing(eventId);

        assertFalse(result);

        verify(redisTemplate).hasKey(processedKey);

        verify(valueOperations).setIfAbsent(
                processingKey,
                "PROCESSING",
                Duration.ofMinutes(5)
        );
    }

    @Test
    void shouldMarkTheEventAsProcessedWithA24HourExpiration() {
        UUID eventId = UUID.randomUUID();

        String processedKey = "processed:event:" + eventId;

        when(redisTemplate.opsForValue())
                .thenReturn(valueOperations);

        service.markProcessed(eventId);

        verify(valueOperations).set(
                processedKey,
                "COMPLETED",
                Duration.ofHours(24)
        );
    }

    @Test
    void shouldDeleteTheProcessingClaimWhenTheEventIsMarkedAsProcessed() {
        UUID eventId = UUID.randomUUID();

        String processingKey = "processing:event:" + eventId;

        when(redisTemplate.opsForValue())
                .thenReturn(valueOperations);

        service.markProcessed(eventId);

        verify(redisTemplate).delete(processingKey);
    }

    @Test
    void shouldReleaseTheProcessingClaim() {
        UUID eventId = UUID.randomUUID();

        String processingKey = "processing:event:" + eventId;

        service.releaseClaim(eventId);

        verify(redisTemplate).delete(processingKey);
    }

}