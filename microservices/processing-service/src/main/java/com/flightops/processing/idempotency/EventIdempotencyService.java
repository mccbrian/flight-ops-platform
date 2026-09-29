package com.flightops.processing.idempotency;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

/**
 * Provides idempotency guarantees for event processing using Redis.
 * <p>
 * Events are first claimed for processing using a temporary processing key
 * to prevent concurrent processing. Once processing completes successfully,
 * the event is marked as processed using a separate key with a retention
 * period, preventing duplicate processing of the same event.
 * <p>
 * Processing claims automatically expire after a fixed timeout to allow
 * recovery from abandoned or failed processing attempts.
 */
@Service
@RequiredArgsConstructor
public class EventIdempotencyService {

    private final StringRedisTemplate redisTemplate;

    /**
     * Attempts to claim an event for processing by creating a temporary processing key
     * in the Redis data store. If the event is already marked as processed or another
     * process has an active claim, the method returns false.
     *
     * @param eventId the unique identifier of the event to claim for processing.
     * @return {@code true} if the event was successfully claimed for processing, or
     *         {@code false} if the event is already processed or claimed by another process.
     */
    public boolean claimForProcessing(UUID eventId) {
        String processedKey = "processed:event:" + eventId;
        String processingKey = "processing:event:" + eventId;

        if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
            return false;
        }

        Boolean claimed = redisTemplate.opsForValue()
                .setIfAbsent(processingKey, "PROCESSING", Duration.ofMinutes(5));

        return Boolean.TRUE.equals(claimed);
    }

    /**
     * Marks an event as successfully processed in the Redis data store by setting a key with a 24-hour expiration
     * and removing any temporary claim on the event.
     *
     * @param eventId the unique identifier of the event to mark as processed.
     */
    public void markProcessed(UUID eventId) {
        redisTemplate.opsForValue().set("processed:event:" + eventId, "COMPLETED", Duration.ofHours(24));
        redisTemplate.delete("processing:event:" + eventId);
    }

    /**
     * Releases the claim on an event by removing its associated processing key from the Redis data store.
     * This allows the event to be claimed and processed again in the future if necessary.
     *
     * @param eventId the unique identifier of the event whose claim should be released.
     */
    public void releaseClaim(UUID eventId) {
        redisTemplate.delete("processing:event:" + eventId);
    }

}