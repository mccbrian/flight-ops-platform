package com.flightops.processing.producer;

import com.flightops.contracts.avro.FailedEvent;
import com.flightops.processing.exception.PublishFailureEventException;
import com.flightops.processing.utility.CamelCaseFormatter;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("Failure Event Producer")
@SuppressWarnings({"unchecked", "rawtypes"})
@DisplayNameGeneration(CamelCaseFormatter.class)
class FailureEventProducerTest {

    private static final String RETRY_TOPIC = "flight-ops.ingestion.retry.v1";
    private static final String DLQ_TOPIC = "flight-ops.ingestion.dlq.v1";

    @Mock
    private KafkaTemplate<String, FailedEvent> kafkaTemplate;

    private FailureEventProducer producer;

    @BeforeEach
    void setUp() {
        producer = new FailureEventProducer(kafkaTemplate);

        ReflectionTestUtils.setField(producer, "retryTopic", RETRY_TOPIC);
        ReflectionTestUtils.setField(producer, "dlqTopic", DLQ_TOPIC);
    }

    @Test
    void shouldSendFailedEventToRetryTopic() {
        FailedEvent failedEvent = failedEvent();

        SendResult<String, FailedEvent> sendResult = mock(SendResult.class);

        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(10L);

        when(sendResult.getRecordMetadata()).thenReturn(metadata);

        when(kafkaTemplate.send(RETRY_TOPIC, failedEvent.getAggregateId(), failedEvent))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        producer.sendToRetry(failedEvent);

        verify(kafkaTemplate).send(RETRY_TOPIC, failedEvent.getAggregateId(), failedEvent);
    }

    @Test
    void shouldSendFailedEventToDlqTopic() {
        FailedEvent failedEvent = failedEvent();

        SendResult sendResult = mock(SendResult.class);

        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(10L);

        when(sendResult.getRecordMetadata()).thenReturn(metadata);

        when(kafkaTemplate.send(DLQ_TOPIC, failedEvent.getAggregateId(), failedEvent))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        producer.sendToDlq(failedEvent);

        verify(kafkaTemplate).send(DLQ_TOPIC, failedEvent.getAggregateId(), failedEvent);
    }

    @Test
    void shouldUseAggregateIdAsKafkaMessageKey() {
        FailedEvent failedEvent = failedEvent();

        SendResult sendResult = mock(SendResult.class);

        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(10L);

        when(sendResult.getRecordMetadata()).thenReturn(metadata);

        when(kafkaTemplate.send(RETRY_TOPIC, failedEvent.getAggregateId(), failedEvent))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        producer.sendToRetry(failedEvent);

        verify(kafkaTemplate).send(RETRY_TOPIC, failedEvent.getAggregateId(), failedEvent);
    }

    @Test
    void shouldThrowPublishFailureEventExceptionWhenRetryPublishFails() {
        FailedEvent failedEvent = failedEvent();

        RuntimeException kafkaFailure = new RuntimeException("Kafka unavailable");

        CompletableFuture<SendResult<String, FailedEvent>> failedFuture = new CompletableFuture<>();

        failedFuture.completeExceptionally(kafkaFailure);

        when(kafkaTemplate.send(RETRY_TOPIC, failedEvent.getAggregateId(), failedEvent)).thenReturn(failedFuture);

        PublishFailureEventException exception = assertThrows(
                PublishFailureEventException.class,
                () -> producer.sendToRetry(failedEvent)
        );

        assertTrue(exception.getMessage().contains(RETRY_TOPIC));
        assertTrue(exception.getMessage().contains(failedEvent.getOriginalEventId()));

        assertInstanceOf(ExecutionException.class, exception.getCause());
        assertSame(kafkaFailure, exception.getCause().getCause());
    }

    @Test
    void shouldThrowPublishFailureEventExceptionWhenKafkaTemplateSendFails() {
        FailedEvent failedEvent = failedEvent();

        RuntimeException kafkaFailure = new RuntimeException("Kafka unavailable");

        when(kafkaTemplate.send(DLQ_TOPIC, failedEvent.getAggregateId(), failedEvent)).thenThrow(kafkaFailure);

        PublishFailureEventException exception = assertThrows(
                PublishFailureEventException.class,
                () -> producer.sendToDlq(failedEvent)
        );

        assertTrue(exception.getMessage().contains(DLQ_TOPIC));
        assertTrue(exception.getMessage().contains(failedEvent.getOriginalEventId()));

        assertSame(kafkaFailure, exception.getCause());
    }

    private FailedEvent failedEvent() {
        return FailedEvent.newBuilder()
                .setOriginalEventId(UUID.randomUUID().toString())
                .setOriginalEventType("FLIGHT_OPERATION_EVENT")
                .setAggregateId("1001")
                .setCorrelationId(UUID.randomUUID().toString())
                .setFailureType("NON_RETRYABLE")
                .setErrorCodes(List.of("FLIGHT_NOT_FOUND"))
                .setReason("Flight does not exist")
                .setRawPayload("{}")
                .setAttemptCount(1)
                .setMaxAttempts(3)
                .setFailedAt(Instant.now())
                .build();
    }
}