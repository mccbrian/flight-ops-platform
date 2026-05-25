package com.flightops.contracts.enums;

/**
 * Enum representing various types of operations or events
 * that can occur within the context of flight operations.
 * <p>
 * The {@code OperationType} enum is used to classify and convey
 * the nature of specific operational changes or statuses
 * associated with flights, such as delays, gate changes,
 * cancellations, or rescheduling events.
 * <p>
 * This enum is primarily utilized in conjunction with classes
 * or methods that process events, validate payloads, or
 * maintain the operational integrity of flight-related data.
 * <p>
 * The available operation types are:
 * <ul>
 *     <li>{@code DELAY} - Indicates a delay in the flight schedule.</li>
 *     <li>{@code GATE_CHANGE} - Represents a change in the assigned gate.</li>
 *     <li>{@code CANCELLED} - Denotes a cancellation of the flight.</li>
 *     <li>{@code RESCHEDULED} - Specifies that the flight has been rescheduled.</li>
 * </ul>
 */
public enum OperationType {
    DELAY,
    GATE_CHANGE,
    CANCELLED,
    RESCHEDULED;
}
