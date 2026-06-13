package com.flightops.processing.mapper;

import com.flightops.contracts.ingestion.FlightOperationEvent;
import com.flightops.processing.dto.Payload;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface FlightOperationEventMapper {

    FlightOperationEvent toFlightOperationEvent(Payload payload);

}
