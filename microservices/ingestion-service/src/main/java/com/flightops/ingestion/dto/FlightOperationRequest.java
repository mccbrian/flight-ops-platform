package com.flightops.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.flightops.contracts.enums.OperationType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.io.Serializable;
import java.time.Instant;

/**
 * Request payload used to submit a flight operation event for ingestion.
 *
 * <p>
 * This DTO represents a flight operation change originating from an external
 * system. It is received by the ingestion API and transformed into a
 * {@code FlightOperationEvent}, which is then published to downstream
 * systems via an event streaming platform.
 * </p>
 *
 * <p>
 * Each request describes a single operational update for a flight, such as
 * a delay, gate change, cancellation, or other status transition.
 * </p>
 *
 * <p>
 * The client is responsible for providing the authoritative event timestamp
 * via {@link #eventTime}. This timestamp represents when the event occurred
 * in the source system and is used for downstream ordering and auditing.
 * </p>
 *
 * <p>
 * Validation rules:
 * </p>
 * <ul>
 *     <li>{@code flightId} must be provided and non-null.</li>
 *     <li>{@code operationType} must be provided and non-null.</li>
 *     <li>{@code status} must be non-null and not blank.</li>
 *     <li>{@code gate} must be non-null and not blank.</li>
 *     <li>{@code delayMinutes}, when provided, must be a positive integer.</li>
 *     <li>{@code reason} must be non-null and not blank.</li>
 *     <li>{@code eventTime} should represent the time the event occurred in the
 *     source system.</li>
 * </ul>
 *
 * @param flightId the unique identifier of the flight; must not be {@code null}
 * @param operationType the type of flight operation being reported; must not
 *                      be {@code null}
 * @param status the current operational status of the flight; must not be
 *               {@code null} or blank
 * @param gate the gate assigned to the flight; must not be {@code null} or blank
 * @param delayMinutes the delay duration in minutes; must not be {@code null} and
 *                     should be a positive integer when provided
 * @param reason the reason for the flight operation or status change; must not
 *               be {@code null} or blank
 * @param eventTime the timestamp indicating when the event occurred in the
 *                  originating system; must not be {@code null} and should
 *                  be formatted as {@code yyyy-MM-dd'T'HH:mm:ssX} (UTC)
 */
public record FlightOperationRequest(
        @NotNull(message = "Flight id is required")
        Integer flightId,
        @NotNull(message = "Operation type is required")
        OperationType operationType,
        @NotBlank(message = "Status is required and cannot be blank")
        String status,
        @NotBlank(message = "Gate is required and cannot be blank")
        String gate,
        @NotNull(message = "Delay minutes is required")
        @Positive(message = "Delay minutes must be a positive integer")
        Integer delayMinutes,
        @NotBlank(message = "Reason is required and cannot be blank")
        String reason,
        @NotNull(message = "Event time is required")
        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssX", timezone = "UTC")
        Instant eventTime
) implements Serializable {}