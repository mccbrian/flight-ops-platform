package com.flightops.ingestion.configuration;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Configuration class for setting up observation-related beans.
 * <p>
 * This class provides configurations to enable observation features, such as tracing or monitoring, for Kafka messaging
 * infrastructure. It defines a KafkaTemplate specifically for handling messages of type FlightOperationEnvelope.
 * Observability is enabled on the KafkaTemplate using an ObservationRegistry.
 * <p>
 * Beans declared in this configuration class allow for integration with observation and telemetry systems to monitor
 * message production and ensure proper traceability.
 */
@Configuration
public class ObservationConfiguration {

    @Bean
    public KafkaTemplate<String, FlightOperationEnvelope> flightOperationKafkaTemplate(
            ProducerFactory<String, FlightOperationEnvelope> producerFactory,
            ObservationRegistry observationRegistry
    ) {
        KafkaTemplate<String, FlightOperationEnvelope> template = new KafkaTemplate<>(producerFactory);

        template.setObservationEnabled(true);
        template.setObservationRegistry(observationRegistry);

        return template;
    }

}