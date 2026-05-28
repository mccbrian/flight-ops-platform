# Flight Ops Platform

## Overview

Flight Ops Platform is a distributed flight operations processing platform designed around event-driven architecture, resilient asynchronous workflows, and operational correctness.

The platform simulates the kinds of operational systems commonly found in large-scale airline and transportation environments, where high-volume operational events—such as flight delays, gate changes, cancellations, and status transitions—must be processed reliably, validated against authoritative relational data, and propagated safely across distributed services.

Rather than focusing on CRUD-centric application design, this platform emphasizes:

* asynchronous event processing
* distributed systems reliability
* claim-based idempotency
* durable processing guarantees
* retry and dead-letter recovery
* structured failure contracts
* operational state management
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
* creating correlation identifiers
* publishing events to Kafka

The service intentionally does **not** perform business persistence. This keeps ingestion fast, stateless, and horizontally scalable.

## Kafka Event Backbone

Kafka serves as the asynchronous event backbone of the platform.

The platform currently uses:

* ingestion topic
* retry topic
* dead-letter topic

Kafka is treated as more than transport infrastructure. The architecture is intentionally evolving toward:

* replayability
* recovery workflows
* resilient decoupling
* operational tracing
* durable event contracts

## Processing Service

The Processing Service is the operational core of the platform.

Responsibilities include:

* controlled event deserialization
* event orchestration
* Redis-based processing claims
* PostgreSQL-backed durable idempotency
* deep validation against relational data
* operational state persistence
* retry and DLQ routing
* failed event recovery

The service intentionally separates:

* transport concerns
* orchestration concerns
* business persistence concerns

to maintain clearer operational boundaries.

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
Controlled Parsing
    ↓
Claim-Based Idempotency
    ↓
Deep Validation
    ↓
Persistence
    ↓
Mark Processed
```

Failures are classified and routed intentionally rather than being treated as generic exceptions.


# Controlled Deserialization Strategy

One of the key architectural decisions in the platform is the use of controlled deserialization.

Rather than relying on Kafka type headers or automatic generic deserialization, the Processing Service consumes raw JSON payloads and explicitly controls:

* envelope parsing
* payload conversion
* validation sequencing
* failure handling

This approach improves:

* predictability
* debugging clarity
* contract evolution
* operational safety

The design evolved directly from debugging real serialization and deserialization failures encountered during development.


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

rather than being silently discarded.


# Structured Failure Contracts

The platform uses structured `FailedEvent` contracts for retry and DLQ processing.

Failure contracts preserve:

* original event identifiers
* correlation IDs
* failure classifications
* validation codes
* original payloads
* failure timestamps

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

The architecture intentionally avoids consumer-to-consumer dependencies by centralizing orchestration through a dedicated coordinator.


# CQRS Direction

The platform is intentionally evolving toward CQRS-style separation.

Current architectural direction includes:

## Write Side

* normalized operational persistence
* authoritative validation
* transactional consistency

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

## Messaging

* Apache Kafka (KRaft mode)

## Data

* PostgreSQL
* Redis

## Infrastructure

* Docker
* Docker Compose


# Local Development

## Start Kafka

```bash
docker compose up -d
```

## Start Redis

```bash
docker compose up -d redis
```

## Start Services

```bash
./mvnw spring-boot:run
```


# Repository Structure

```text
flight-ops-platform/
│
├── infrastructure/
│   ├── kafka/
│   └── redis/
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


# Future Enhancements

The platform roadmap includes:

* retry attempt tracking
* exponential backoff
* delayed retry scheduling
* replay tooling
* outbox pattern implementation
* Avro and schema registry integration
* distributed tracing
* metrics and observability
* projection rebuild pipelines
* stream processing
* optimistic concurrency controls
* operational dashboards


# Architectural Notes

This project intentionally emphasizes:

* operational correctness
* failure recovery
* orchestration boundaries
* event contract stability
* resilient asynchronous processing

over simplistic “happy path” implementations.

The goal is to model how distributed operational systems evolve in production environments where failure handling, recoverability, and correctness are first-class concerns.
