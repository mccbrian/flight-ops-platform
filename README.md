# Flight Ops Platform

[![Flight Ops CI](https://github.com/mccbrian/flight-ops-platform/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/mccbrian/flight-ops-platform/actions/workflows/ci.yml)

## Overview

Flight Ops Platform is a distributed flight operations processing platform designed around event-driven architecture, resilient asynchronous workflows, and operational correctness.

The platform simulates the kinds of operational systems commonly found in large-scale airline and transportation environments, where high-volume operational events must be processed reliably, validated against authoritative relational data, and propagated safely across distributed services.

Rather than focusing on CRUD-centric application design, this platform emphasizes:

* asynchronous event processing
* distributed systems reliability
* claim-based idempotency
* durable processing guarantees
* retry and dead-letter recovery
* structured failure contracts
* Avro-based schema governance
* operational state management
* metrics and tracing observability
* CQRS-oriented architectural direction

The project evolved incrementally through real failure handling and iterative architectural refinement, intentionally mirroring how production systems mature under operational pressure.

# Architectural Goals

The primary goal of the platform is not simply to “use Kafka,” but to explore how resilient distributed systems are designed when correctness, recoverability, and operational safety matter.

The architecture prioritizes:

* separation of ingestion from processing
* explicit orchestration boundaries
* resilient asynchronous workflows
* controlled event deserialization
* durable idempotency guarantees
* failure classification and recovery
* recoverable dead-letter contracts
* schema-governed event contracts
* retry attempt limits and failure escalation
* operational observability readiness

This repository intentionally avoids overly simplistic “happy path” examples in favor of production-shaped processing behavior.

# High-Level Architecture

```text
┌─────────────────────┐
│  Ingestion Service  │
│---------------------│
│ REST API            │
│ Request Validation  │
│ Event Publication   │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│       Kafka         │
│---------------------│
│ Event Backbone      │
│ Retry Topics        │
│ DLQ Topics          │
│ Schema Registry     │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Processing Service  │
│---------------------│
│ Controlled Parsing  │
│ Idempotency         │
│ Validation          │
│ Persistence         │
│ Retry Recovery      │
│ Failure Routing     │
│ Metrics + Tracing   │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ PostgreSQL + Redis  │
│---------------------│
│ Durable State       │
│ Operational Models  │
│ Fast Claim Tracking │
└─────────────────────┘
```

# Core Components

## Ingestion Service

The Ingestion Service acts as the edge boundary of the platform.

Responsibilities include:

* accepting operational requests via REST
* lightweight request validation
* generating event metadata
* propagating correlation identifiers through HTTP responses, logging context, and Kafka headers
* publishing events to Kafka

The service intentionally does **not** perform business persistence. To keep ingestion fast, stateless, and horizontally scalable.

## Kafka Event Backbone

Kafka serves as the asynchronous event backbone of the platform.

The platform currently uses:

* ingestion topic
* retry topic
* dead-letter topic
* Avro schemas
* Schema Registry

Kafka is treated as more than transport infrastructure. The architecture is intentionally evolving toward:

* replayability
* recovery workflows
* resilient decoupling
* operational tracing
* durable event contracts
* schema-governed producer/consumer compatibility

## Schema Registry

Schema Registry provides contract governance for Kafka messages exchanged between services.

The platform currently uses Avro schemas for:

* flight operation envelopes
* flight operation payloads
* failed event contracts

This allows the platform to evolve beyond JVM-local serialization assumptions and toward explicit, versioned message contracts that can be validated across producers and consumers.

## Processing Service

The Processing Service is the operational core of the platform.

Responsibilities include:

* controlled event deserialization
* event orchestration
* Redis-based processing claims
* PostgreSQL-backed durable idempotency
* deep validation against relational data
* operational state persistence
* event-time ordering to prevent stale updates from overwriting newer state
* retry and DLQ routing
* retry attempt tracking
* failed event recovery
* manual Kafka offset acknowledgment
* Micrometer processing metrics
* correlation-aware logging and tracing context

The service intentionally separates:

* transport concerns
* orchestration concerns
* business persistence concerns

to maintain clearer operational boundaries.

Processing orchestration is centralized through an `EventProcessingCoordinator`, while Kafka consumers remain thin transport adapters that delegate work to the shared processing flow.

## PostgreSQL

PostgreSQL acts as the durable system of record.

The schema combines:

* transactional flight data
* reference data
* operational state
* read-side projections
* event infrastructure

The design intentionally supports CQRS-oriented evolution by separating:

* normalized write-side state
* denormalized read-side projections
* event publication infrastructure

Operational processing validates against authoritative relational data rather than relying solely on event payload trust.


## Redis

Redis is used as a fast in-memory processing coordination layer.

Responsibilities include:

* temporary processing claims
* duplicate suppression
* lightweight operational coordination

Redis is intentionally not treated as the durable source of truth. Durable correctness remains enforced by PostgreSQL.

# Observability

The platform includes observability support through Spring Boot Actuator, Micrometer, Prometheus, Grafana, and Zipkin infrastructure.

Both services expose Actuator endpoints and tracing configuration. The Ingestion Service propagates correlation identifiers through request/response handling and Kafka headers, while the Processing Service enriches logs with event, aggregate, correlation, trace, and span context.

The Processing Service currently records metrics for:

* successfully processed events
* failed events
* retry-routed events
* DLQ-routed events
* duplicate events
* processing latency

These metrics provide a foundation for future operational dashboards, alerting rules, service-level monitoring, and trace-aware failure investigation.


# Event Processing Flow

The current event lifecycle follows this general flow:

```text
Client Request
    ↓
Ingestion Service
    ↓
Kafka Event Publication
    ↓
Processing Consumer
    ↓
Manual Offset Control
    ↓
Controlled Avro Mapping
    ↓
Claim-Based Idempotency
    ↓
Deep Validation
    ↓
Persistence
    ↓
Mark Processed
    ↓
Acknowledge Offset
```

Failures are classified and routed intentionally rather than being treated as generic exceptions.


# Controlled Deserialization Strategy

One of the key architectural decisions in the platform is the use of controlled deserialization.

Rather than relying on JVM-local type headers or implicit generic deserialization, the platform now uses Avro-backed contracts with Schema Registry while still explicitly controlling the internal processing flow.

The Processing Service controls:

* Avro envelope mapping
* internal event representation
* payload conversion
* validation sequencing
* failure handling

This approach improves:

* predictability
* debugging clarity
* contract evolution
* operational safety

The design evolved directly from debugging real serialization and deserialization failures encountered during development, moving from raw JSON stabilization toward schema-governed Avro contracts.


# Idempotency Strategy

The platform implements a layered idempotency model.

## Redis Claim Layer

Redis acts as a temporary processing claim system:

* prevents concurrent duplicate processing
* coordinates active event handling
* releases claims on failure

Events are not considered “processed” simply because they were seen.

## PostgreSQL Durable Layer

PostgreSQL provides durable idempotency guarantees through persisted processed-event tracking.

An event is only considered complete after:

* validation succeeds
* persistence succeeds
* operational updates succeed

This separation prevents false-positive completion states during partial failures.


# Failure Handling Strategy

The platform intentionally distinguishes between:

* retryable failures
* non-retryable failures

Examples include:

| Failure Type                           | Classification |
| -------------------------------------- | -------------- |
| transient infrastructure issues        | retryable      |
| relational validation failures         | non-retryable  |
| malformed payloads                     | non-retryable  |
| temporary downstream processing issues | retryable      |

Failures are routed to:

* retry topics
* dead-letter topics

rather than being silently discarded. Retryable failures are bounded by a configured maximum attempt count before being escalated to the DLQ.


# Structured Failure Contracts

The platform uses structured Avro `FailedEvent` contracts for retry and DLQ processing.

Failure contracts preserve:

* original event identifiers
* correlation IDs
* failure classifications
* validation codes
* original payloads
* failure timestamps
* attempt counts
* maximum retry thresholds

This enables:

* replayability
* operational debugging
* future analytics
* failure dashboards
* recovery tooling


# Retry Recovery

Retry handling is implemented as an intentional recovery workflow rather than a simple retry loop.

Failed retryable events are:

* consumed separately
* reconstructed from failure contracts
* reprocessed through the standard orchestration path
* retried with incremented attempt counts
* escalated to DLQ after the configured retry threshold

The architecture intentionally avoids consumer-to-consumer dependencies by centralizing orchestration through a dedicated coordinator.


# CQRS Direction

The platform is intentionally evolving toward CQRS-style separation.

Current architectural direction includes:

## Write Side

* normalized operational persistence
* authoritative validation
* transactional consistency
* event-time ordering to prevent stale updates

## Read Side

* projection-oriented operational views
* denormalized operational dashboards
* future replay/rebuild support

Examples of future projection use cases:

* departure boards
* delay dashboards
* gate utilization
* operational analytics


# Technology Stack

## Backend

* Java 25
* Spring Boot
* Spring Kafka
* Spring Data JDBC
* Flyway
* MapStruct
* Micrometer

## Messaging

* Apache Kafka (KRaft mode)
* Avro
* Schema Registry

## Data

* PostgreSQL
* Redis

## Infrastructure

* Docker
* Docker Compose
* Prometheus
* Grafana
* Zipkin


# Local Development

Java 25, Maven 3.9+, Docker, and Docker Compose are required. The services run locally, while Kafka and supporting infrastructure run in containers. PostgreSQL is an existing local installation containing the `flights` database; the sample dataset is not packaged in this repository.

## Start Kafka and Schema Registry

```bash
docker compose -f infrastructure/kafka/docker-compose.yaml up -d
bash infrastructure/kafka/scripts/create-topics.sh
```

## Start Redis

```bash
docker compose -f infrastructure/redis/docker-compose.yaml up -d
```

## Start Observability Infrastructure

```bash
docker compose -f infrastructure/zipkin/docker-compose.yaml up -d
docker compose -f infrastructure/prometheus/docker-compose.yaml up -d
docker compose -f infrastructure/grafana/docker-compose.yaml up -d
```

The Prometheus Compose file currently references a machine-specific absolute path for `prometheus.yaml`; update that mount when running the project from a different directory.

## Start Services

Build and install the shared Avro contracts first:

```bash
mvn -f shared-libraries/event-contracts/pom.xml clean install
```

Configure the Processing Service environment variables (`SERVER_PORT`, `KAFKA_HOST`, `KAFKA_PORT`, `SCHEMA_REGISTRY_URL`, `DATASOURCE_URL`, `DATASOURCE_USERNAME`, `DATASOURCE_PASSWORD`, `REDIS_HOST`, and `REDIS_PORT`) through the shell or IDE. The database credentials are not embedded in the README.

Run the services in separate terminals from the repository root:

```bash
mvn -f microservices/ingestion-service/pom.xml spring-boot:run
```

```bash
mvn -f microservices/processing-service/pom.xml spring-boot:run
```

The Ingestion Service exposes `POST /api/v1/flight-operations` on port `8081`. Successful publication returns `202 Accepted`, meaning Kafka accepted the event for asynchronous processing rather than confirming database persistence.


# Testing and Verification

Both services include unit tests, and the platform has integration tests using Testcontainers. Tests cover the ingestion API and Kafka publication as well as the processing lifecycle.

The processing test suite exercises:

* durable persistence and duplicate-event handling
* Redis claims and concurrent claim behavior
* retry recovery, attempt limits, and DLQ escalation
* Kafka consumer acknowledgment behavior
* stale event rejection and optimistic update semantics

Run the test suites after installing the shared contracts:

```bash
mvn -f microservices/ingestion-service/pom.xml verify
mvn -f microservices/processing-service/pom.xml verify
```

Docker must be running for Testcontainers-backed integration tests. The repository contains these tests; successful results depend on the local build environment.


# Repository Structure

```text
flight-ops-platform/
│
├── infrastructure/
│   ├── kafka/
│   ├── redis/
│   ├── prometheus/
│   ├── grafana/
│   └── zipkin/
│
├── microservices/
│   ├── ingestion-service/
│   └── processing-service/
│
├── shared-libraries/
│   └── event-contracts/
│
└── README.md
```


# Key Design Decisions

The platform's architecture reflects deliberate design decisions focused on reliable event processing, clear service 
boundaries, and operational correctness. The following summarizes the reasoning behind those choices.

| Decision | Benefit |
| --- | --- |
| Separate ingestion and processing services | Keeps API acceptance independent of database processing |
| Kafka with Avro and Schema Registry | Durable event transport with explicit contracts |
| Redis claims backed by PostgreSQL completion records | Reduces in-flight duplicate work while preserving durable deduplication |
| Manual offset acknowledgment | Lets consumers acknowledge only after a handled outcome |
| Bounded retry and DLQ recovery | Preserves failed events without unbounded application-level retries |
| Spring Data JDBC and transactional writes | Keeps persistence explicit and business state consistent with processed-event records |
| Event-time checks and optimistic locking | Prevents stale snapshots and conflicting updates |
| Single-node local Kafka and separately deployed tools | Keeps development reproducible and easy to inspect |