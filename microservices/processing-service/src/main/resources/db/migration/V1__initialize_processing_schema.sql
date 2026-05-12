-- ============================================================
-- SCHEMA: flight_operations
-- ============================================================
-- Contains operational data and event-processing metadata
-- for flight status management within the FlightOps domain.
CREATE SCHEMA if NOT EXISTS flight_operations;


-- ============================================================
-- TABLE: processed_events
-- ============================================================
-- Stores events that have already been processed to ensure
-- idempotency in event-driven workflows.
--
-- Purpose:
-- Prevents duplicate handling of the same event, which is
-- critical in distributed systems where retries may occur.
--
-- Columns:
--   event_id      : Unique identifier of the event (UUID).
--   event_type    : Type/category of the event (e.g., FlightDelayed).
--   aggregate_id  : Identifier of the related aggregate (e.g., flight_id).
--   processed_at  : Timestamp when the event was processed.
--
-- Notes:
-- - Acts as a guard against reprocessing.
-- - Typically checked before applying state changes.
CREATE TABLE flight_operations.processed_events
(
    event_id     uuid PRIMARY KEY,
    event_type   text        NOT NULL,
    aggregate_id text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);


-- ============================================================
-- TABLE: flight_operation_status
-- ============================================================
-- Represents the current operational status of a flight.
-- This is a mutable, write-side projection used by FlightOps.
--
-- Purpose:
-- Holds the latest known state derived from processed events.
--
-- Columns:
--   flight_id     : Unique identifier of the flight.
--   status        : Current status (e.g., SCHEDULED, BOARDING, DELAYED).
--   gate          : Assigned gate for the flight.
--   delay_minutes : Delay duration in minutes.
--   updated_at    : Timestamp of the last update.
--
-- Notes:
-- - This table is updated as new events are processed.
-- - It is not an event store, but a projection of current state.
-- - Designed for fast reads and operational queries.
CREATE TABLE flight_operations.flight_operation_status
(
    flight_id     INTEGER PRIMARY KEY,
    status        text,
    gate          text,
    delay_minutes INTEGER,
    updated_at    timestamptz NOT NULL DEFAULT now()
);