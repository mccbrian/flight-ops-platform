package com.flightops.processing.idempotency;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EventIdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(EventIdempotencyService.class);

    private final StringRedisTemplate redisTemplate;

    /**
     * Checks if the given event is being processed for the first time.
     * Stores the event ID in Redis with a specified time-to-live (TTL)
     * to ensure idempotency for a finite period.
     *
     * @param eventId The unique identifier of the event to be checked.
     * @return {@code true} if this is the first occurrence of the event,
     *         {@code false} if the event was already processed.
     */
    public boolean firstTimeSeen(UUID eventId) {
        String key = "processed:event:" + eventId;

        Boolean inserted = redisTemplate.opsForValue()
                .setIfAbsent(key, "PROCESSING", Duration.ofHours(24));

        if (Boolean.TRUE.equals(inserted)) {
            log.info("New event registered in Redis. eventId={}", eventId);
        }

        return Boolean.TRUE.equals(inserted);
    }

}