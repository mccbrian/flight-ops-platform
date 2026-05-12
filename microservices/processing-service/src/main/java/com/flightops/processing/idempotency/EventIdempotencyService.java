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
     * Attempts to claim an event for processing by marking it in the Redis data store.
     * This ensures that the event is idempotent and won't be processed multiple times concurrently.
     *
     * @param eventId The unique identifier of the event to claim for processing.
     * @return {@code true} if the event was successfully claimed for processing,
     *         {@code false} if the event was already processed or currently being processed.
     */
    public boolean claimForProcessing(UUID eventId) {
        String processedKey = "processed:event:" + eventId;

        if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
            return false;
        }

        String processingKey = "processing:event:" + eventId;

        Boolean claimed = redisTemplate.opsForValue()
                .setIfAbsent(processingKey, "PROCESSING", Duration.ofMinutes(5));

        return Boolean.TRUE.equals(claimed);
    }

    /**
     * Marks an event as processed in the Redis data store by setting a key with a 24-hour expiration
     * and removing any temporary claim on the event.
     *
     * @param eventId The unique identifier of the event to mark as processed.
     */
    public void markProcessed(UUID eventId) {
        redisTemplate.opsForValue()
                .set("processed:event:" + eventId, "COMPLETED", Duration.ofHours(24));

        redisTemplate.delete("processing:event:" + eventId);
    }

    /**
     * Releases the claim on an event by removing its associated processing key from the Redis data store.
     * This allows the event to be claimed and processed again in the future if necessary.
     *
     * @param eventId The unique identifier of the event whose claim should be released.
     */
    public void releaseClaim(UUID eventId) {
        redisTemplate.delete("processing:event:" + eventId);
    }

}