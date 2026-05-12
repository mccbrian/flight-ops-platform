package com.flightops.contracts.ingestion;

import com.flightops.contracts.enums.OperationType;

import java.time.Instant;

public record FlightOperationEvent (
        Integer flightId,
        OperationType operationType,
        String status,
        String gate,
        Integer delayMinutes,
        String reason,
        Instant eventTime
) {}
