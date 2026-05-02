-- ============================================================
-- TABLE: departure_board_projection
-- ============================================================
-- Denormalized read-side table for departure board displays.
-- Updated via events (projection) from FlightOps operations.
create table if not exists flight_operations.departure_board_projection (
    flight_id bigint primary key,
    flight_no varchar(10) not null,
    departure_airport varchar(10) not null,
    arrival_airport varchar(10) not null,
    scheduled_departure timestamptz not null,
    status varchar(50) not null,
    gate varchar(20),
    delay_minutes integer not null default 0,
    updated_at timestamptz not null
);

create index if not exists idx_departure_board_projection_airport_departure
    on flight_operations.departure_board_projection (departure_airport, scheduled_departure);