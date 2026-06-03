package com.flightops.processing.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Provides application-level metrics for tracking flight operation event processing.
 *
 * <p>This component uses Micrometer to publish counters and timers that can be consumed by
 * monitoring systems such as Prometheus, Datadog, or New Relic.</p>
 *
 * <p>Metrics captured include:</p>
 * <ul>
 *   <li>Number of successfully processed events</li>
 *   <li>Number of failed events</li>
 *   <li>Number of events sent to retry</li>
 *   <li>Number of events sent to a dead-letter queue (DLQ)</li>
 *   <li>Number of duplicate events</li>
 *   <li>Processing latency for flight operation events</li>
 * </ul>
 *
 * <p>These metrics are intended for observability, alerting, and operational monitoring of
 * event-processing pipelines.</p>
 */
@Component
public class FlightOperationMetrics {

    private static final String METRIC_PROCESSED_TOTAL = "flight_ops_processed_total";
    private static final String METRIC_FAILED_TOTAL = "flight_ops_failed_total";
    private static final String METRIC_RETRY_TOTAL = "flight_ops_retry_total";
    private static final String METRIC_DLQ_TOTAL = "flight_ops_dlq_total";
    private static final String METRIC_DUPLICATE_TOTAL = "flight_ops_duplicate_total";
    private static final String METRIC_PROCESSING_DURATION = "flight_ops_processing_duration";

    private final MeterRegistry meterRegistry;
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Counter retryCounter;
    private final Counter dlqCounter;
    private final Counter duplicateCounter;
    private final Timer processingTimer;

    public FlightOperationMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.processedCounter = buildCounter(
                meterRegistry,
                METRIC_PROCESSED_TOTAL,
                "Successfully processed events");
        this.failedCounter = buildCounter(
                meterRegistry,
                METRIC_FAILED_TOTAL,
                "Failed events");
        this.retryCounter = buildCounter(
                meterRegistry,
                METRIC_RETRY_TOTAL,
                "Events routed to retry");
        this.dlqCounter = buildCounter(
                meterRegistry,
                METRIC_DLQ_TOTAL,
                "Events routed to DLQ");
        this.duplicateCounter = buildCounter(
                meterRegistry,
                METRIC_DUPLICATE_TOTAL,
                "Events marked as duplicates");
        this.processingTimer = buildTimer(
                meterRegistry
        );
    }

    private Counter buildCounter(MeterRegistry meterRegistry, String name, String description) {
        return Counter.builder(name)
                .description(description)
                .register(meterRegistry);
    }

    private Timer buildTimer(MeterRegistry meterRegistry) {
        return Timer.builder(FlightOperationMetrics.METRIC_PROCESSING_DURATION)
                .description("Time taken to process flight operation events")
                .register(meterRegistry);
    }

    public void incrementProcessed() {
        processedCounter.increment();
    }

    public void incrementFailed() {
        failedCounter.increment();
    }

    public void incrementRetry() {
        retryCounter.increment();
    }

    public void incrementDlq() {
        dlqCounter.increment();
    }

    public void incrementDuplicate() {
        duplicateCounter.increment();
    }

    public Timer.Sample startProcessingTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopProcessingTimer(Timer.Sample sample) {
        sample.stop(processingTimer);
    }

}