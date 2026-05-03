package com.flightops.processing.idempotency;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EventIdempotencyService {

    private final StringRedisTemplate redisTemplate;

    public boolean firstTimeSeen(UUID eventId) {
        String key = "processed:event:" + eventId;
        Boolean inserted = redisTemplate.opsForValue()
                .setIfAbsent(key, "PROCESSING", Duration.ofHours(24));

        return Boolean.TRUE.equals(inserted);
    }

}