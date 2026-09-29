package com.flightops.processing.idempotency;

import com.flightops.processing.utility.CamelCaseFormatter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full-context integration test for {@link EventIdempotencyService}, backed by a real Redis instance (Testcontainers).
 * <p>
 * {@link EventIdempotencyServiceTest} already covers the service's branching logic against a mocked
 * {@code StringRedisTemplate}, so it isn't repeated here. This class instead verifies the properties that only a real
 * Redis instance can prove:
 * <ul>
 *     <li>{@code setIfAbsent} is genuinely atomic, so exactly one of two truly concurrent claims for the same event
 *     succeeds</li>
 *     <li>the processing and processed keys carry the TTLs the service intends (5 minutes / 24 hours), rather than a
 *     mock simply echoing back whatever {@code Duration} was passed to it</li>
 *     <li>claim / release / re-claim and claim / mark-processed / re-claim behave correctly end to end against a real
 *     store</li>
 * </ul>
 * A disposable Postgres instance is also started because this test boots the complete Spring context (Flyway
 * migrations and the JDBC repositories run regardless of what this test actually exercises). Kafka is never touched,
 * so its properties are given harmless placeholder values and listener auto-startup is disabled, rather than starting
 * a real Kafka broker.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "server.port=0",
                "spring.kafka.bootstrap-servers=localhost:9092",
                "spring.kafka.consumer.properties.schema.registry.url=http://localhost:8085",
                "spring.kafka.producer.properties.schema.registry.url=http://localhost:8085",
                "spring.kafka.listener.auto-startup=false"
        }
)
@Testcontainers
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Event Idempotency Service")
class EventIdempotencyServiceIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:18");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private EventIdempotencyService service;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void shouldClaimEventForProcessingWhenNotAlreadyClaimedOrProcessed() {
        UUID eventId = UUID.randomUUID();

        boolean claimed = service.claimForProcessing(eventId);

        assertTrue(claimed);
    }

    @Test
    void shouldNotAllowASecondClaimWhileTheFirstClaimIsStillActive() {
        UUID eventId = UUID.randomUUID();

        boolean firstClaim = service.claimForProcessing(eventId);
        boolean secondClaim = service.claimForProcessing(eventId);

        assertTrue(firstClaim);
        assertFalse(secondClaim);
    }

    @Test
    void shouldAllowReclaimingAnEventAfterItsClaimIsReleased() {
        UUID eventId = UUID.randomUUID();

        service.claimForProcessing(eventId);
        service.releaseClaim(eventId);

        boolean reclaimed = service.claimForProcessing(eventId);

        assertTrue(reclaimed);
    }

    @Test
    void shouldNotAllowClaimingAnEventThatHasAlreadyBeenMarkedProcessed() {
        UUID eventId = UUID.randomUUID();

        service.claimForProcessing(eventId);
        service.markProcessed(eventId);

        boolean claimAfterProcessed = service.claimForProcessing(eventId);

        assertFalse(claimAfterProcessed);
    }

    @Test
    void shouldSetAProcessingClaimThatExpiresWithinFiveMinutes() {
        UUID eventId = UUID.randomUUID();

        service.claimForProcessing(eventId);

        Long expireSeconds = redisTemplate.getExpire(processingKey(eventId), TimeUnit.SECONDS);

        assertNotNull(expireSeconds);
        assertTrue(expireSeconds > 0);
        assertTrue(expireSeconds <= Duration.ofMinutes(5).toSeconds());
    }

    @Test
    void shouldMarkAnEventAsProcessedWithATwentyFourHourExpirationAndClearTheProcessingClaim() {
        UUID eventId = UUID.randomUUID();

        service.claimForProcessing(eventId);
        service.markProcessed(eventId);

        Long expireSeconds = redisTemplate.getExpire(processedKey(eventId), TimeUnit.SECONDS);

        assertNotNull(expireSeconds);
        assertTrue(expireSeconds > 0);
        assertTrue(expireSeconds <= Duration.ofHours(24).toSeconds());

        assertNotEquals(Boolean.TRUE, redisTemplate.hasKey(processingKey(eventId)));
    }

    @Test
    void shouldOnlyAllowOneOfTwoTrulyConcurrentClaimsForTheSameEventToSucceed() throws InterruptedException {
        UUID eventId = UUID.randomUUID();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch readyLatch = new CountDownLatch(2);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {
            Callable<Boolean> claimTask = () -> {
                readyLatch.countDown();
                startLatch.await();
                return service.claimForProcessing(eventId);
            };
            /*
            Submit both tasks first, so neither call to claimForProcessing() happens until both threads have
            signaled they're ready and are released at the same instant by startLatch. This is what makes the
            test an actual test of setIfAbsent's atomicity, rather than two sequential calls that happen to be on
            different threads.
             */
            List<Future<Boolean>> futures = List.of(
                    executor.submit(claimTask),
                    executor.submit(claimTask)
            );

            readyLatch.await();
            startLatch.countDown();

            long successfulClaims = futures.stream()
                    .map(this::getResult)
                    .filter(Boolean::booleanValue)
                    .count();

            assertEquals(1, successfulClaims);
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean getResult(Future<Boolean> future) {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private String processingKey(UUID eventId) {
        return "processing:event:" + eventId;
    }

    private String processedKey(UUID eventId) {
        return "processed:event:" + eventId;
    }

}