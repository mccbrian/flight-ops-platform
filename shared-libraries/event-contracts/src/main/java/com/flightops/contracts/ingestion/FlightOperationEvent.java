package com.flightops.contracts.ingestion;

import com.flightops.contracts.enums.OperationType;

import java.io.Serializable;
import java.time.Instant;

public record FlightOperationEvent (

        Integer flightId,
        OperationType operationType, // DELAY, GATE_CHANGE, etc.
        String status,
        String gate,
        Instant eventTime

) implements Serializable {}
