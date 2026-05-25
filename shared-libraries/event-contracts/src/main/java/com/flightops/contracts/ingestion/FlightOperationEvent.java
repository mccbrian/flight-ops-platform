package com.flightops.contracts.ingestion;

import com.flightops.contracts.enums.OperationType;

import java.time.Instant;


/**
 * Represents an event that captures an operation or status change associated
 * with a specific flight. This record is used to encapsulate details about
 * various flight-related events, such as delays, gate changes, cancellations,
 * or reschedules.
 *
 * @param flightId       The unique identifier of the flight.
 * @param operationType  The type of operation being performed, as defined by
 *                       the {@link OperationType} enum.
 * @param status         The current operational status of the flight.
 * @param gate           The gate associated with the flight, if applicable.
 * @param delayMinutes   The duration of the delay for the flight, in minutes.
 * @param reason         The reason for the event or operation change.
 * @param eventTime      The timestamp indicating when the event was generated
 *                       or occurred.
 */
public record FlightOperationEvent (
        Integer flightId,
        OperationType operationType,
        String status,
        String gate,
        Integer delayMinutes,
        String reason,
        Instant eventTime
) {}
