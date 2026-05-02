-- ============================================================
-- TABLE: flight_operation_status
-- ============================================================
-- Tracks the current operational state of a flight.
-- This is the mutable write-side representation used by FlightOps.
create table if not exists flight_operations.flight_operation_status (
    flight_id bigint primary key,
    status varchar(50) not null,
    gate varchar(20),
    delay_minutes integer not null default 0,
    reason varchar(100),
    last_updated_at timestamptz not null,
    version integer not null default 0
);

create index if not exists idx_flight_operation_status_status
    on flight_operations.flight_operation_status (status);