package com.flightops.processing.mapper;

import com.flightops.contracts.avro.FlightOperationEnvelope;
import com.flightops.contracts.avro.FlightOperationEvent;
import com.flightops.contracts.enums.EventType;
import com.flightops.contracts.enums.OperationType;
import com.flightops.processing.dto.EventEnvelopeJson;
import com.flightops.processing.dto.Payload;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.UUID;

@Mapper(componentModel = "spring")
public interface FlightOperationMapper {

    @Mapping(target = "eventType", source = "eventType")
    EventEnvelopeJson toEventEnvelopeJson(FlightOperationEnvelope source);

    @Mapping(target = "operationType", source = "operationType")
    Payload toPayload(FlightOperationEvent source);

    default UUID map(String value) {
        return value == null ? null : UUID.fromString(value);
    }

    default EventType mapEventType(String value) {
        return value == null ? null : EventType.valueOf(value);
    }

    default OperationType mapOperationType(String value) {
        return value == null ? null : OperationType.valueOf(value);
    }

}