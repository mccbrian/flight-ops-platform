package com.flightops.processing.dto;

import com.flightops.contracts.enums.OperationType;

import java.time.Instant;

/**
 * Represents the payload of a flight operation event.
 * <p>
 * This record encapsulates key details about a specific flight event, including its operational context, current status,
 * and any reasons or delays associated with it.
 * <p>
 * Fields:
 *<ul>
 *    <li>{@code flightId}: The unique identifier of the flight associated with the event.</li>
 *    <li>{@code operationType}: The type of operation or event, classified by {@code OperationType}.</li>
 *    <li>{@code status}: The current status of the flight (e.g., "Scheduled", "Departed").</li>
 *    <li>{@code gate}: The assigned gate for the flight at the time of the event.</li>
 *    <li>{@code delayMinutes}: The number of minutes by which the flight is delayed.</li>
 *    <li>{@code reason}: The reason for the delay or status change, if any.</li>
 *    <li>{@code eventTime}: The timestamp indicating when the event occurred.</li>
 *</ul>
 */
public record Payload(
        Integer flightId,
        OperationType operationType,
        String status,
        String gate,
        Integer delayMinutes,
        String reason,
        Instant eventTime
) {}