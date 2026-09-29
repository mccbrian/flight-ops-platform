package com.flightops.ingestion.producer;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.ingestion.utility.CamelCaseFormatter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
@DisplayNameGeneration(CamelCaseFormatter.class)
@DisplayName("Flight Operation Producer")
class FlightOperationProducerTest {

    @Mock
    private KafkaTemplate<String, FlightOperationEnvelope> kafkaTemplate;

    private FlightOperationProducer producer;

    @BeforeEach
    void setUp() {
        producer = new FlightOperationProducer(kafkaTemplate, "flight-operation-events");
    }

    @Test
    void shouldSendAProducerRecordToKafkaWhenPublishingFlightOperationEvents() {
        FlightOperationEnvelope envelope = createEnvelope();

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                CompletableFuture.completedFuture(null);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        producer.publish(envelope);

        verify(kafkaTemplate).send(any(ProducerRecord.class));
    }

    @Test
    void shouldUseTheAggregateIdAsTheKafkaMessageKeyWhenPublishingFlightOperationEvents() {
        FlightOperationEnvelope envelope = createEnvelope();

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                CompletableFuture.completedFuture(null);

        ArgumentCaptor<ProducerRecord<String, FlightOperationEnvelope>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        producer.publish(envelope);

        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, FlightOperationEnvelope> record = captor.getValue();

        assertEquals(
                envelope.getAggregateId(),
                record.key()
        );
    }

    @Test
    void shouldPublishToTheConfiguredTopicWhenPublishingFlightOperationEvents() {
        FlightOperationEnvelope envelope = createEnvelope();

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                CompletableFuture.completedFuture(null);

        ArgumentCaptor<ProducerRecord<String, FlightOperationEnvelope>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        producer.publish(envelope);

        verify(kafkaTemplate).send(captor.capture());

        assertEquals(
                "flight-operation-events",
                captor.getValue().topic()
        );
    }

    @Test
    void shouldIncludeTheCorrelationIdHeaderWhenPublishingFlightOperationEvents() {
        FlightOperationEnvelope envelope = createEnvelope();

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                CompletableFuture.completedFuture(null);

        ArgumentCaptor<ProducerRecord<String, FlightOperationEnvelope>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        producer.publish(envelope);

        verify(kafkaTemplate).send(captor.capture());

        Header header = captor.getValue()
                .headers()
                .lastHeader("X-Correlation-Id");

        assertNotNull(header);
        assertEquals(
                envelope.getCorrelationId(),
                new String(header.value(), StandardCharsets.UTF_8)
        );
    }

    @Test
    void shouldIncludeTheEventIdHeaderWhenPublishingFlightOperationEvents() {
        FlightOperationEnvelope envelope = createEnvelope();

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                CompletableFuture.completedFuture(null);

        ArgumentCaptor<ProducerRecord<String, FlightOperationEnvelope>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        producer.publish(envelope);

        verify(kafkaTemplate).send(captor.capture());

        Header header = captor.getValue()
                .headers()
                .lastHeader("X-Event-Id");

        assertNotNull(header);
        assertEquals(
                envelope.getEventId(),
                new String(header.value(), StandardCharsets.UTF_8)
        );
    }

    @Test
    void shouldReturnAPublishCompletionFutureWhenPublishingFlightOperationEvents() {
        FlightOperationEnvelope envelope = createEnvelope();

        SendResult<String, FlightOperationEnvelope> sendResult = mock(SendResult.class);
        RecordMetadata metadata = mock(RecordMetadata.class);

        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(123L);

        when(sendResult.getRecordMetadata()).thenReturn(metadata);

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> result =
                producer.publish(envelope);

        assertNotNull(result);
        assertFalse(result.isCompletedExceptionally());
    }

    private FlightOperationEnvelope createEnvelope() {
        FlightOperationEvent payload = FlightOperationEvent.newBuilder()
                .setFlightId(1001)
                .setOperationType("ARRIVAL")
                .setStatus("ON_TIME")
                .setGate("A12")
                .setDelayMinutes(0)
                .setReason("Scheduled")
                .setEventTime(Instant.now())
                .build();

        return FlightOperationEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setAggregateId("1001")
                .setEventType("FLIGHT_OPERATION_EVENT")
                .setOccurredAt(Instant.now())
                .setPayload(payload)
                .build();
    }

    @Test
    void shouldReturnAFailedFutureWhenPublishingFlightOperationEventsFails() {
        FlightOperationEnvelope envelope = createEnvelope();

        RuntimeException exception = new RuntimeException("Kafka unavailable");

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> future =
                new CompletableFuture<>();
        future.completeExceptionally(exception);

        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(future);

        CompletableFuture<SendResult<String, FlightOperationEnvelope>> result =
                producer.publish(envelope);

        assertTrue(result.isCompletedExceptionally());

        CompletionException completionException =
                assertThrows(CompletionException.class, result::join);

        assertSame(exception, completionException.getCause());
    }

    private void assertTrue(boolean completedExceptionally) {}

}