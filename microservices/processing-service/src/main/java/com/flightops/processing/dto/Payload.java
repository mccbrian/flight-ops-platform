package com.flightops.processing.dto;

import com.flightops.contracts.enums.OperationType;

import java.time.Instant;

public record Payload(
        Integer flightId,
        OperationType operationType,
        String status,
        String gate,
        Integer delayMinutes,
        String reason,
        Instant eventTime
) {}