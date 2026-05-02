-- ============================================================
-- SCHEMA: bookings
-- ============================================================
-- This schema represents a flight booking and operations system.
-- It combines:
--   1. Core transactional data (bookings, tickets, flights)
--   2. Reference data (airports, airplanes, routes)
--   3. Operational state (flight_operation_status)
--   4. Read-side projections (departure_board_projection)
--   5. Event-driven infrastructure (outbox_events)
--
-- The design supports an event-driven architecture with CQRS:
--   - Write-side: normalized transactional tables
--   - Read-side: denormalized projections
--   - Integration: outbox pattern for reliable messaging
-- ============================================================


-- ============================================================
-- REFERENCE DATA TABLES
-- ============================================================
BEGIN;

-- ------------------------------------------------------------
-- TABLE: airplanes_data
-- ------------------------------------------------------------
-- Stores aircraft metadata used across the system.
-- Acts as reference/lookup data.
CREATE TABLE IF NOT EXISTS bookings.airplanes_data
(
    airplane_code character(3) COLLATE pg_catalog."default" NOT NULL,
    model jsonb NOT NULL,
    range integer NOT NULL,
    speed integer NOT NULL,
    CONSTRAINT airplanes_data_pkey PRIMARY KEY (airplane_code)
);

COMMENT ON TABLE bookings.airplanes_data
    IS 'Airplanes (internal multilingual data)';

COMMENT ON COLUMN bookings.airplanes_data.airplane_code
    IS 'Airplane code, IATA';

COMMENT ON COLUMN bookings.airplanes_data.model
    IS 'Airplane model';

COMMENT ON COLUMN bookings.airplanes_data.range
    IS 'Maximum flight range, km';

COMMENT ON COLUMN bookings.airplanes_data.speed
    IS 'Cruise speed, km/h';

-- ------------------------------------------------------------
-- TABLE: airports_data
-- ------------------------------------------------------------
-- Stores airport metadata including location and timezone.
-- Used for route planning and display.
CREATE TABLE IF NOT EXISTS bookings.airports_data
(
    airport_code character(3) COLLATE pg_catalog."default" NOT NULL,
    airport_name jsonb NOT NULL,
    city jsonb NOT NULL,
    country jsonb NOT NULL,
    coordinates point NOT NULL,
    timezone text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT airports_data_pkey PRIMARY KEY (airport_code)
);

COMMENT ON TABLE bookings.airports_data
    IS 'Airports (internal multilingual data)';

COMMENT ON COLUMN bookings.airports_data.airport_code
    IS 'Airport code, IATA';

COMMENT ON COLUMN bookings.airports_data.airport_name
    IS 'Airport name';

COMMENT ON COLUMN bookings.airports_data.city
    IS 'City';

COMMENT ON COLUMN bookings.airports_data.country
    IS 'Country';

COMMENT ON COLUMN bookings.airports_data.coordinates
    IS 'Airport coordinates (longitude and latitude)';

COMMENT ON COLUMN bookings.airports_data.timezone
    IS 'Airport time zone';

-- ============================================================
-- CORE TRANSACTIONAL TABLES
-- ============================================================

-- ------------------------------------------------------------
-- TABLE: bookings
-- ------------------------------------------------------------
-- Represents a customer booking containing one or more tickets.
CREATE TABLE IF NOT EXISTS bookings.bookings
(
    book_ref character(6) COLLATE pg_catalog."default" NOT NULL,
    book_date timestamp with time zone NOT NULL,
    total_amount numeric(10, 2) NOT NULL,
    CONSTRAINT bookings_pkey PRIMARY KEY (book_ref)
);

COMMENT ON TABLE bookings.bookings
    IS 'Bookings';

COMMENT ON COLUMN bookings.bookings.book_ref
    IS 'Booking number';

COMMENT ON COLUMN bookings.bookings.book_date
    IS 'Booking date';

COMMENT ON COLUMN bookings.bookings.total_amount
    IS 'Total booking amount';

-- ------------------------------------------------------------
-- TABLE: tickets
-- ------------------------------------------------------------
-- Represents a passenger ticket within a booking.
CREATE TABLE IF NOT EXISTS bookings.tickets
(
    ticket_no text COLLATE pg_catalog."default" NOT NULL,
    book_ref character(6) COLLATE pg_catalog."default" NOT NULL,
    passenger_id text COLLATE pg_catalog."default" NOT NULL,
    passenger_name text COLLATE pg_catalog."default" NOT NULL,
    outbound boolean NOT NULL,
    CONSTRAINT tickets_pkey PRIMARY KEY (ticket_no),
    CONSTRAINT tickets_book_ref_passenger_id_outbound_key UNIQUE (book_ref, passenger_id, outbound)
);

COMMENT ON TABLE bookings.tickets
    IS 'Tickets';

COMMENT ON COLUMN bookings.tickets.ticket_no
    IS 'Ticket number';

COMMENT ON COLUMN bookings.tickets.book_ref
    IS 'Booking number';

COMMENT ON COLUMN bookings.tickets.passenger_id
    IS 'Passenger ID';

COMMENT ON COLUMN bookings.tickets.passenger_name
    IS 'Passenger name';

COMMENT ON COLUMN bookings.tickets.outbound
    IS 'Outbound flight?';

-- ------------------------------------------------------------
-- TABLE: flights
-- ------------------------------------------------------------
-- Represents a scheduled flight instance.
-- This is a core domain entity in the operations domain.
CREATE TABLE IF NOT EXISTS bookings.flights
(
    flight_id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    route_no text COLLATE pg_catalog."default" NOT NULL,
    status text COLLATE pg_catalog."default" NOT NULL,
    scheduled_departure timestamp with time zone NOT NULL,
    scheduled_arrival timestamp with time zone NOT NULL,
    actual_departure timestamp with time zone,
    actual_arrival timestamp with time zone,
    CONSTRAINT flights_pkey PRIMARY KEY (flight_id),
    CONSTRAINT flights_route_no_scheduled_departure_key UNIQUE (route_no, scheduled_departure)
);

COMMENT ON TABLE bookings.flights
    IS 'Flights';

COMMENT ON COLUMN bookings.flights.flight_id
    IS 'Flight ID';

COMMENT ON COLUMN bookings.flights.route_no
    IS 'Route number';

COMMENT ON COLUMN bookings.flights.status
    IS 'Flight status';

COMMENT ON COLUMN bookings.flights.scheduled_departure
    IS 'Scheduled departure time';

COMMENT ON COLUMN bookings.flights.scheduled_arrival
    IS 'Scheduled arrival time';

COMMENT ON COLUMN bookings.flights.actual_departure
    IS 'Actual departure time';

COMMENT ON COLUMN bookings.flights.actual_arrival
    IS 'Actual arrival time';

-- ------------------------------------------------------------
-- TABLE: segments
-- ------------------------------------------------------------
-- Represents a flight segment (leg) associated with a ticket.
CREATE TABLE IF NOT EXISTS bookings.segments
(
    ticket_no text COLLATE pg_catalog."default" NOT NULL,
    flight_id integer NOT NULL,
    fare_conditions text COLLATE pg_catalog."default" NOT NULL,
    price numeric(10, 2) NOT NULL,
    CONSTRAINT segments_pkey PRIMARY KEY (ticket_no, flight_id)
);

COMMENT ON TABLE bookings.segments
    IS 'Flight segment (leg)';

COMMENT ON COLUMN bookings.segments.ticket_no
    IS 'Ticket number';

COMMENT ON COLUMN bookings.segments.flight_id
    IS 'Flight ID';

COMMENT ON COLUMN bookings.segments.fare_conditions
    IS 'Travel class';

COMMENT ON COLUMN bookings.segments.price
    IS 'Travel price';

-- ------------------------------------------------------------
-- TABLE: boarding_passes
-- ------------------------------------------------------------
-- Represents seat assignment and boarding details.
CREATE TABLE IF NOT EXISTS bookings.boarding_passes
(
    ticket_no text COLLATE pg_catalog."default" NOT NULL,
    flight_id integer NOT NULL,
    seat_no text COLLATE pg_catalog."default" NOT NULL,
    boarding_no integer,
    boarding_time timestamp with time zone,
    CONSTRAINT boarding_passes_pkey PRIMARY KEY (ticket_no, flight_id),
    CONSTRAINT boarding_passes_flight_id_boarding_no_key UNIQUE (flight_id, boarding_no),
    CONSTRAINT boarding_passes_flight_id_seat_no_key UNIQUE (flight_id, seat_no)
);

COMMENT ON TABLE bookings.boarding_passes
    IS 'Boarding passes';

COMMENT ON COLUMN bookings.boarding_passes.ticket_no
    IS 'Ticket number';

COMMENT ON COLUMN bookings.boarding_passes.flight_id
    IS 'Flight ID';

COMMENT ON COLUMN bookings.boarding_passes.seat_no
    IS 'Seat number';

COMMENT ON COLUMN bookings.boarding_passes.boarding_no
    IS 'Boarding pass number';

COMMENT ON COLUMN bookings.boarding_passes.boarding_time
    IS 'Boarding time';

-- ------------------------------------------------------------
-- TABLE: seats
-- ------------------------------------------------------------
-- Defines seat configurations per aircraft.
CREATE TABLE IF NOT EXISTS bookings.seats
(
    airplane_code character(3) COLLATE pg_catalog."default" NOT NULL,
    seat_no text COLLATE pg_catalog."default" NOT NULL,
    fare_conditions text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT seats_pkey PRIMARY KEY (airplane_code, seat_no)
);

COMMENT ON TABLE bookings.seats
    IS 'Seats';

COMMENT ON COLUMN bookings.seats.airplane_code
    IS 'Airplane code, IATA';

COMMENT ON COLUMN bookings.seats.seat_no
    IS 'Seat number';

COMMENT ON COLUMN bookings.seats.fare_conditions
    IS 'Travel class';

-- ============================================================
-- ROUTING / SCHEDULING
-- ============================================================

-- ------------------------------------------------------------
-- TABLE: routes
-- ------------------------------------------------------------
-- Defines flight routes and scheduling templates.
-- Used to derive flight instances and enrich read models.
CREATE TABLE IF NOT EXISTS bookings.routes
(
    route_no text COLLATE pg_catalog."default" NOT NULL,
    validity tstzrange NOT NULL,
    departure_airport character(3) COLLATE pg_catalog."default" NOT NULL,
    arrival_airport character(3) COLLATE pg_catalog."default" NOT NULL,
    airplane_code character(3) COLLATE pg_catalog."default" NOT NULL,
    days_of_week integer[] NOT NULL,
    scheduled_time time without time zone NOT NULL,
    duration interval NOT NULL
);

COMMENT ON TABLE bookings.routes
    IS 'Routes';

COMMENT ON COLUMN bookings.routes.route_no
    IS 'Route number';

COMMENT ON COLUMN bookings.routes.validity
    IS 'Period of validity';

COMMENT ON COLUMN bookings.routes.departure_airport
    IS 'Airport of departure';

COMMENT ON COLUMN bookings.routes.arrival_airport
    IS 'Airport of arrival';

COMMENT ON COLUMN bookings.routes.airplane_code
    IS 'Airplane code, IATA';

COMMENT ON COLUMN bookings.routes.days_of_week
    IS 'Days of week array';

COMMENT ON COLUMN bookings.routes.scheduled_time
    IS 'Scheduled local time of departure';

COMMENT ON COLUMN bookings.routes.duration
    IS 'Estimated duration';

-- ============================================================
-- MIGRATION TRACKING
-- ============================================================

-- ------------------------------------------------------------
-- TABLE: flyway_schema_history
-- ------------------------------------------------------------
-- Tracks database schema migrations.
CREATE TABLE IF NOT EXISTS bookings.flyway_schema_history
(
    installed_rank integer NOT NULL,
    version character varying(50) COLLATE pg_catalog."default",
    description character varying(200) COLLATE pg_catalog."default" NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    script character varying(1000) COLLATE pg_catalog."default" NOT NULL,
    checksum integer,
    installed_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    installed_on timestamp without time zone NOT NULL DEFAULT now(),
    execution_time integer NOT NULL,
    success boolean NOT NULL,
    CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank)
);

-- ============================================================
-- RELATIONSHIPS (FOREIGN KEYS)
-- ============================================================

-- Booking relationships
ALTER TABLE IF EXISTS bookings.boarding_passes
    ADD CONSTRAINT boarding_passes_ticket_no_flight_id_fkey FOREIGN KEY (ticket_no, flight_id)
    REFERENCES bookings.segments (ticket_no, flight_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS boarding_passes_pkey
    ON bookings.boarding_passes(ticket_no, flight_id);

ALTER TABLE IF EXISTS bookings.routes
    ADD CONSTRAINT routes_airplane_code_fkey FOREIGN KEY (airplane_code)
    REFERENCES bookings.airplanes_data (airplane_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS bookings.routes
    ADD CONSTRAINT routes_arrival_airport_fkey FOREIGN KEY (arrival_airport)
    REFERENCES bookings.airports_data (airport_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS bookings.routes
    ADD CONSTRAINT routes_departure_airport_fkey FOREIGN KEY (departure_airport)
    REFERENCES bookings.airports_data (airport_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS bookings.seats
    ADD CONSTRAINT seats_airplane_code_fkey FOREIGN KEY (airplane_code)
    REFERENCES bookings.airplanes_data (airplane_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE CASCADE;

ALTER TABLE IF EXISTS bookings.segments
    ADD CONSTRAINT segments_flight_id_fkey FOREIGN KEY (flight_id)
    REFERENCES bookings.flights (flight_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS segments_flight_id_idx
    ON bookings.segments(flight_id);

ALTER TABLE IF EXISTS bookings.segments
    ADD CONSTRAINT segments_ticket_no_fkey FOREIGN KEY (ticket_no)
    REFERENCES bookings.tickets (ticket_no) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS bookings.tickets
    ADD CONSTRAINT tickets_book_ref_fkey FOREIGN KEY (book_ref)
    REFERENCES bookings.bookings (book_ref) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
-- ============================================================
-- END OF SCHEMA
-- ============================================================
END;