package com.flightops.ingestion.dto;

import com.flightops.contracts.enums.OperationType;

import java.io.Serializable;
import java.time.Instant;

public record FlightOperationRequest(
        Integer flightId,
        OperationType operationType,
        String status,
        String gate,
        Integer delayMinutes,
        String reason,
        Instant eventTime
) implements Serializable {}