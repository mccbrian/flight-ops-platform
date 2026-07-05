package com.flightops.processing.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProcessingExceptionHandler {

    public void handleConsumerException(
            String topic,
            String eventId,
            String correlationId,
            String aggregateId,
            Exception exception
    ) {
        log.error(
                "kafka_consumer_error topic={} eventId={} correlationId={} aggregateId={} exceptionType={} message={}",
                topic,
                eventId,
                correlationId,
                aggregateId,
                exception.getClass().getSimpleName(),
                exception.getMessage(),
                exception
        );
    }

}