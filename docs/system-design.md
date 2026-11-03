# System Design Document: The Unified Service Scheduler

- **Challenge Scenario:** Scenario A – The Unified Service Scheduler (*Domain: Ownership*)
- **Implemented Service Layer:** Backend (REST API, PostgreSQL, Redis, Kafka via Transactional Outbox)
- **Mocked Service Layer:** Client (OpenAPI 3 / Swagger UI, cURL scripts) — plus Kafka UI to inspect published events
- **Document Version:** 2.1 — restored from v2.0 at `4069e20`, retaining its architectural analysis and original AI collaboration narrative; implementation status updated after the review completion.

**Reading guide:** Implemented means present in the current source and covered by the verification record. Designed-only means a proposed extension, not an available endpoint or guarantee. The design retains future-facing analysis rather than deleting it. See [verification.md](verification.md) for observed test/demo results.

---

## Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [Scope: Implemented vs. Designed-only](#2-scope-implemented-vs-designed-only)
3. [Assumptions & Business Rules](#3-assumptions--business-rules)
4. [System Architecture](#4-system-architecture)
5. [Component Roles & Responsibilities](#5-component-roles--responsibilities)
6. [Data Architecture](#6-data-architecture)
7. [Concurrency & Correctness Strategy](#7-concurrency--correctness-strategy)
8. [Detailed Data Flows](#8-detailed-data-flows)
9. [API Contract](#9-api-contract)
10. [Technology Stack & Justifications](#10-technology-stack--justifications)
11. [Observability Strategy](#11-observability-strategy)
12. [Reliability & Failure Modes](#12-reliability--failure-modes)
13. [Scalability Path](#13-scalability-path)
14. [Testing Strategy](#14-testing-strategy)
15. [Generative AI Collaboration in the Design Phase](#15-generative-ai-collaboration-in-the-design-phase)

---

## 1. Executive Summary

The **Unified Service Scheduler** replaces manual workshop booking sheets with a backend service that lets a customer or service advisor book a maintenance appointment for a **vehicle (VIN)**, **service type** and **dealership** at a **desired start time**.

Before confirming, the service checks — for the *entire* service duration — that it can allocate both:

1. an `ACTIVE` **ServiceBay** at that dealership, and
2. an `ACTIVE` **Technician** at that dealership holding the skill the `ServiceType` requires.

On success it persists a `CONFIRMED` **Appointment** linking customer, vehicle, technician and bay, and publishes an `AppointmentConfirmed` event for downstream domains (notifications, parts, workshop board).

**Design principle:** *PostgreSQL is the single source of truth for correctness; Redis and Kafka are accelerators and integration channels.* If Redis is down, bookings still succeed (slower). If Kafka is down, bookings still succeed and events are delivered when it recovers.

---

## 2. Scope: Implemented vs. Designed-only

| Capability | Status |
| :--- | :--- |
| `POST /appointments` with automatic bay + qualified technician allocation | **Implemented** |
| Double-booking prevention (dealership advisory lock + GiST exclusion constraints) | **Implemented** |
| Operating-hours / past-time validation with dealership timezone (DST-safe) | **Implemented** |
| Idempotent create (`Idempotency-Key`, Postgres-backed, Redis replay cache) | **Designed-only** |
| `GET /appointments/{id}` | **Implemented** |
| `POST /appointments/{id}/cancel` | **Designed-only** |
| `GET /dealerships/{id}/availability?date=&serviceTypeId=` + Redis availability cache | **Designed-only** |
| Redis read-through dealership/service-type catalog cache, 1-hour TTL, fail-open | **Implemented** |
| Transactional Outbox → Kafka (`confirmed` CloudEvent) | **Implemented**; `cancelled` event **Designed-only** |
| Kafka UI (kafbat) to inspect topic, keys, headers, payloads and consumer lag | **Implemented** (Docker Compose) |
| Demo downstream consumer (logs received events, shows consumer group in Kafka UI) | **Implemented** |
| Actuator health/readiness and ECS JSON booking/outbox/cache logs | **Implemented** |
| Custom metrics (Prometheus), OpenTelemetry tracing, optional Grafana LGTM stack | **Designed-only** |
| AuthN/AuthZ (OAuth2 / dealer-scoped tokens) | Designed-only (§13) |
| Technician shifts, leave, dealership holidays, lunch breaks | Designed-only (assumption §3.6) |
| Rescheduling, partial-day closures, multi-bay jobs | Out of scope |
| Real Customer / Vehicle master data | Out of scope — owned by CRM domain (§3.5) |

---

## 3. Assumptions & Business Rules

1. **Service duration & skill.** Each `ServiceType` (e.g. `OIL_CHANGE`, `BRAKE_INSPECTION`, `EV_BATTERY_DIAGNOSTICS`) has a fixed `duration_minutes` and exactly one `required_skill`.
2. **Half-open intervals.** An appointment occupies `[start, start + duration)`. Back-to-back bookings (09:00–10:00 and 10:00–11:00) do **not** conflict. Two intervals overlap iff `startA < endB AND endA > startB`.
3. **Auto-allocation.** The client never picks a bay or technician. The service picks:
   - **Bay:** first free active bay ordered by `bay_code` (bays are assumed interchangeable).
   - **Technician:** among free, active, qualified technicians, the one with the **fewest skills** first (tie-break by name, id). This keeps scarce specialists (e.g. EV-certified) free for jobs only they can do.
4. **Time & operating hours (Implemented).** API timestamps are ISO-8601 instants, normalized to UTC and microsecond precision. Each dealership has an IANA timezone and daily `open_time`/`close_time` (local). The full elapsed appointment interval must fit inside that local day's opening/closing instants. Exact opening starts and closing ends are accepted. A nonexistent or ambiguous opening/closing boundary is rejected rather than silently shifted; elapsed intervals spanning DST are supported when boundaries are unambiguous. Past starts and outside-hours requests → `422`. Starts/ends beyond `9999-12-31T23:59:59.999999Z` → `400 INVALID_START_TIME`.
5. **Customer & vehicle ownership.** Customer and vehicle master data belong to the CRM / Ownership master domain. The scheduler stores `customer_id` (external reference) and a validated `vehicle_vin` (17 chars, ISO 3779 alphabet — no `I`, `O`, `Q`) on the appointment. This satisfies "associating customer and vehicle" without duplicating another domain's data.
6. **Working calendar.** Dealerships are open every day with the same hours; technicians are available whenever the dealership is open. Shifts/holidays are a designed extension (an `unavailability` table subtracted in the same queries).
7. **Vehicle rule.** One vehicle cannot have two overlapping `CONFIRMED` appointments — across any dealership.
8. **Cancellation (Designed-only).** The target behavior is `CANCELLED`, immediate resource release and an `AppointmentCancelled` event. Current schema permits `CONFIRMED` only. Implementing cancellation requires status-aware overlap queries and `WHERE status='CONFIRMED'` exclusion predicates in a new migration.
9. **Availability is advisory (Designed-only endpoint).** A future availability read model cannot reserve a slot; confirmation remains authoritative.
10. **Idempotency (Designed-only).** Proposed optional `Idempotency-Key`: same key/body replays the original appointment; changed body rejects with `422`. Current requests are not idempotent; after a timeout callers can receive a vehicle-overlap conflict on retry.

---

## 4. System Architecture

### 4.1. High-Level Architecture (Mermaid)

```mermaid
graph TB
    subgraph Client_Layer ["Client (mocked)"]
        Swagger["Swagger UI / OpenAPI 3"]
        CLI["cURL scripts / Integration tests"]
    end

    subgraph Backend ["Unified Service Scheduler (Java 21 / Spring Boot 4)"]
        API["REST API<br/>AppointmentController<br/>ApiExceptionHandler (RFC 9457)<br/>AvailabilityController: Designed-only"]
        Booking["BookingService<br/>tx orchestration"]
        Policy["Domain: TimeSlot · OperatingHoursPolicy<br/>SlotFinder: Designed-only"]
        Repo["JDBC Repositories<br/>advisory lock · allocation SQL"]
        Cache["CatalogService: Implemented<br/>AvailabilityCache · IdempotencyService: Designed-only"]
        Relay["OutboxRelay (@Scheduled)"]
        DemoConsumer["Demo consumer<br/>(stands in for Notification svc)"]
    end

    subgraph Infra ["Docker Compose"]
        PG[("PostgreSQL 16<br/>source of truth<br/>exclusion constraints · outbox")]
        Redis[("Redis 7.4<br/>catalog cache: Implemented<br/>availability / replay: Designed-only")]
        Kafka{{"Kafka 4 (KRaft)<br/>keyloop.ownership.appointments.v1"}}
        KUI["Kafka UI (kafbat)<br/>:8081"]
    end

    Swagger --> API
    CLI --> API
    API --> Booking
    Booking --> Policy
    Booking --> Cache
    Cache <--> Redis
    Booking --> Repo
    Repo <--> PG
    Relay -->|claim PENDING rows| PG
    Relay -->|publish CloudEvent| Kafka
    Kafka --> DemoConsumer
    KUI --> Kafka
```

### 4.2. ASCII Fallback

This preserves the v2.0 target view; availability/idempotency cache paths are Designed-only as labeled above.

```text
 Swagger UI / cURL ──HTTP JSON──▶ ┌──────────────── Spring Boot service ────────────────┐
                                  │ Controllers ─▶ BookingService ─▶ Domain policies    │
                                  │                   │      │                          │
                                  │     Redis caches ◀┘      └▶ JDBC repos ─────────────┼──▶ PostgreSQL 16
                                  │  (catalog/avail/idem)        (advisory lock,        │     appointments
                                  │                               exclusion constr.)    │     outbox_events
                                  │ OutboxRelay ◀── claim PENDING ──────────────────────┼──── (same DB)
                                  │      │                                              │
                                  └──────┼──────────────────────────────────────────────┘
                                         ▼
                                  Kafka (KRaft) ──▶ demo consumer
                                         ▲
                                  Kafka UI :8081 (browse messages, headers, lag)
```

---

## 5. Component Roles & Responsibilities

| Component | Responsibility |
| :--- | :--- |
| **AppointmentController (Implemented)** | Create/read contract and Bean Validation. AvailabilityController, cancel and `Idempotency-Key` handling are Designed-only. |
| **ApiExceptionHandler (Implemented)** | RFC 9457 Problem Details with stable `errorCode`; rejection logs omit customer/VIN. `traceId` correlation is Designed-only. |
| **BookingService (Implemented)** | Catalog lookup → policy validation → dealership lock → VIN/resource checks → appointment + outbox insert in one transaction → confirmation log only after commit. Idempotency, cancel, availability invalidation and custom metrics are Designed-only. |
| **Domain (`TimeSlot`, `OperatingHoursPolicy`) (Implemented)** | Pure interval and timezone-aware hours rules. `SlotFinder` availability computation is Designed-only. |
| **BookingRepository (Implemented)** | Combines the original ResourceRepository / AppointmentRepository responsibilities: explicit JdbcClient lock, allocation, overlap, insert and lookup SQL. |
| **CatalogService** | Read-through Redis cache for dealership and service-type metadata (fail-open on Redis errors). |
| **AvailabilityService + AvailabilityCache (Designed-only)** | Proposed date slot calculation and versioned cache invalidation on booking/cancel. |
| **IdempotencyService (Designed-only)** | Proposed Redis replay cache and authoritative PostgreSQL unique key; neither exists in the current schema. |
| **OutboxRepository / OutboxRelay (Implemented)** | Every 500 ms drains at most 10 events, claiming one per short transaction; publishes outside the transaction; then token-fenced mark/retry. Reclaims leases after 30 s and fails after 10 attempts. Claim-one avoids later rows consuming their leases while earlier sends wait. |
| **AppointmentEventsLoggingConsumer** | Demo downstream consumer (group `notification-service-demo`); proves end-to-end delivery and shows consumer lag in Kafka UI. |
| **Kafka UI (kafbat)** | Browser view of topic messages: timestamp, partition/offset, key, headers (`ce_type`, `traceparent`), JSON body. |

---

## 6. Data Architecture

### 6.1. Entity-Relationship Diagram

**Target model:** `APPOINTMENTS.CANCELLED`, `idempotency_key`, `request_hash`, `cancelled_at`, and outbox `aggregate_type`/`trace_parent` below are **Designed-only**. Current V1/V3 migrations are the authoritative implemented schema. V3 additionally stores `claim_token` and `next_attempt_at` for fenced completion and backoff.

```mermaid
erDiagram
    DEALERSHIPS ||--o{ SERVICE_BAYS : owns
    DEALERSHIPS ||--o{ TECHNICIANS : employs
    DEALERSHIPS ||--o{ APPOINTMENTS : hosts
    SERVICE_TYPES ||--o{ APPOINTMENTS : "duration & skill"
    SERVICE_BAYS ||--o{ APPOINTMENTS : "allocated to"
    TECHNICIANS ||--o{ TECHNICIAN_SKILLS : holds
    TECHNICIANS ||--o{ APPOINTMENTS : "assigned to"

    DEALERSHIPS {
        UUID id PK
        VARCHAR name
        VARCHAR timezone "IANA, e.g. Europe/London"
        TIME open_time
        TIME close_time
    }
    SERVICE_TYPES {
        UUID id PK
        VARCHAR code UK
        VARCHAR name
        INT duration_minutes
        VARCHAR required_skill
    }
    SERVICE_BAYS {
        UUID id PK
        UUID dealership_id FK
        VARCHAR bay_code "UK per dealership"
        VARCHAR status "ACTIVE | MAINTENANCE"
    }
    TECHNICIANS {
        UUID id PK
        UUID dealership_id FK
        VARCHAR full_name
        VARCHAR status "ACTIVE | INACTIVE"
    }
    TECHNICIAN_SKILLS {
        UUID technician_id PK, FK
        VARCHAR skill_code PK
    }
    APPOINTMENTS {
        UUID id PK
        UUID dealership_id FK
        UUID service_type_id FK
        UUID service_bay_id FK
        UUID technician_id FK
        VARCHAR customer_id "external CRM ref"
        VARCHAR vehicle_vin
        TIMESTAMPTZ start_time
        TIMESTAMPTZ end_time
        VARCHAR status "CONFIRMED | CANCELLED"
        VARCHAR idempotency_key UK
        VARCHAR request_hash
        TIMESTAMPTZ created_at
        TIMESTAMPTZ cancelled_at
    }
    OUTBOX_EVENTS {
        UUID id PK "= CloudEvent id"
        VARCHAR aggregate_type
        UUID aggregate_id
        VARCHAR event_type
        JSONB payload
        VARCHAR trace_parent
        VARCHAR status "PENDING | IN_FLIGHT | PUBLISHED | FAILED"
        INT attempts
        TEXT last_error
        TIMESTAMPTZ created_at
        TIMESTAMPTZ claimed_at
        TIMESTAMPTZ published_at
    }
```

### 6.2. Integrity Constraints (defense in depth)

**Implemented now:** V1 uses `appointments_bay_no_overlap`, `appointments_technician_no_overlap`, and `appointments_vin_no_overlap`. Since the only legal status is `CONFIRMED`, all rows participate. Composite foreign keys also enforce that allocated resources belong to the appointment's dealership.

**Designed-only target when cancellation/idempotency are added:** the following filtered exclusions and unique idempotency key must be introduced together with status-aware queries. They are not the current DDL.

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- inside CREATE TABLE appointments
CONSTRAINT excl_appointments_bay_overlap  EXCLUDE USING gist
    (service_bay_id WITH =, tstzrange(start_time, end_time, '[)') WITH &&) WHERE (status = 'CONFIRMED'),
CONSTRAINT excl_appointments_tech_overlap EXCLUDE USING gist
    (technician_id  WITH =, tstzrange(start_time, end_time, '[)') WITH &&) WHERE (status = 'CONFIRMED'),
CONSTRAINT excl_appointments_vin_overlap  EXCLUDE USING gist
    (vehicle_vin    WITH =, tstzrange(start_time, end_time, '[)') WITH &&) WHERE (status = 'CONFIRMED'),
CONSTRAINT uq_appointments_idempotency_key UNIQUE (idempotency_key)
```

Even if application logic had a bug, the database physically cannot store a double-booked bay, technician or vehicle.

### 6.3. Redis Key Design

Redis is an **accelerator only** — every Redis call is wrapped so that a Redis failure degrades to a DB read, never to an error.

| Key | Type | TTL | Purpose |
| :--- | :--- | :--- | :--- |
| `scheduler:catalog:dealership:{id}` | STRING (JSON) | 1 h | **Implemented:** timezone & hours |
| `scheduler:catalog:service-type:{id}` | STRING (JSON) | 1 h | **Implemented:** duration & skill |
| `scheduler:availability:version:{dealershipId}:{date}` | STRING (counter) | 2 d | **Designed-only:** bump on booking/cancel |
| `scheduler:cache:availability:{dealershipId}:{date}:{serviceTypeId}:v{version}` | STRING (JSON) | 60 s | **Designed-only:** versioned slots, old versions expire without SCAN/DEL storms |
| `scheduler:idempotency:{key}` | STRING (JSON) | 24 h | **Designed-only:** requestHash/appointmentId replay |

Catalog cache entries are validated, not negatively cached, and use 100 ms connection/command timeouts. Read/write failures and corrupt entries fall back to PostgreSQL. A future catalog administration endpoint must evict affected entries or accept metadata staleness for up to one hour; no administration workflow is implemented.

### 6.4. Kafka Topic & Events

- **Topic:** `keyloop.ownership.appointments.v1` (3 partitions locally).
- **Key:** `appointmentId` — routes one appointment to one partition and distributes dealerships across partitions. (v1.1 used `dealershipId`; rejected because a large dealership becomes a hot partition.) Future confirmed/cancelled ordering also needs per-aggregate relay sequencing; the key alone does not order independent workers/producers.
- **Headers:** `ce_type`, `ce_id` **Implemented**. `traceparent` propagation is **Designed-only**.
- **Delivery:** at-least-once, stable CloudEvent `id` (= outbox row id) across retries. A production consumer must durably deduplicate this id. The implemented demo consumer only logs identifiers; it does not perform or deduplicate business side effects.
- **Producer:** `acks=all`, `enable.idempotence=true`.

Event types: `com.keyloop.ownership.appointment.confirmed.v1` **Implemented**; `com.keyloop.ownership.appointment.cancelled.v1` **Designed-only**. The example below illustrates the complete event shape; service catalog values are dynamic. Demo seed service IDs end in `401` (oil) and `402` (brakes), not `403` (illustrative EV service).

```json
{
  "specversion": "1.0",
  "id": "0f1e0f7c-3a43-4c5e-9d0e-6c2b8f5b1a10",
  "source": "/keyloop/ownership/unified-service-scheduler",
  "type": "com.keyloop.ownership.appointment.confirmed.v1",
  "subject": "9b2d8f10-4c3a-41b2-8d9e-1a2b3c4d5e6f",
  "time": "2030-01-06T08:15:30Z",
  "datacontenttype": "application/json",
  "data": {
    "appointmentId": "9b2d8f10-4c3a-41b2-8d9e-1a2b3c4d5e6f",
    "dealershipId": "11111111-1111-1111-1111-111111111111",
    "dealershipName": "Keyloop Central Motors - London",
    "customerId": "CUST-90812",
    "vehicleVin": "WBA3A5C51CF256789",
    "serviceType": { "id": "44444444-4444-4444-4444-444444444403", "code": "EV_BATTERY_DIAGNOSTICS",
                     "name": "EV High-Voltage Battery Diagnostics", "durationMinutes": 120,
                     "requiredSkill": "EV_HIGH_VOLTAGE_CERT" },
    "allocatedServiceBay": { "id": "22222222-2222-2222-2222-222222222201", "bayCode": "BAY-01" },
    "assignedTechnician": { "id": "33333333-3333-3333-3333-333333333301", "fullName": "Alex Mercer" },
    "startTime": "2030-01-07T09:00:00Z",
    "endTime": "2030-01-07T11:00:00Z",
    "status": "CONFIRMED",
    "createdAt": "2030-01-06T08:15:30Z",
    "cancelledAt": null
  }
}
```

---

## 7. Concurrency & Correctness Strategy

### 7.1. Chosen approach

Inside a single `READ COMMITTED` transaction:

1. `SELECT pg_advisory_xact_lock(hashtextextended(:dealershipId, 0))` — serializes bookings **per dealership**. Released automatically on commit/rollback.
2. **Designed-only:** re-check a future `Idempotency-Key` in the DB. Current implementation performs no idempotency-key processing.
3. VIN overlap check → `409 VEHICLE_ALREADY_BOOKED`.
4. Select first free active bay → else `409 NO_SERVICE_BAY_AVAILABLE`.
5. Select best free qualified technician → else `409 NO_QUALIFIED_TECHNICIAN_AVAILABLE`.
6. Insert appointment + outbox row; commit.
7. Implemented exclusion violations are translated to the matching `409`, including same VIN at **two different dealerships**. `23505` idempotency replay handling is **Designed-only**.

Because each statement in `READ COMMITTED` takes a fresh snapshot, every query after step 1 sees all appointments committed by previous lock holders.

### 7.2. Why not the v1.1 design (`FOR UPDATE SKIP LOCKED` on bays)

| Problem | Explanation |
| :--- | :--- |
| **False "no bay" (409) under load** | Row locks on `service_bays` are not tied to a time range. With 1 bay, a 14:00 booking holding the lock makes a concurrent 09:00 booking skip the bay and fail although 09:00 is free. |
| **Lock does not protect the predicate** | Availability is derived from `appointments`, not from the bay row. If transaction A commits and releases the bay lock while B's statement is running, B's `NOT EXISTS` sub-query may use a snapshot without A's insert, lock the bay and insert an overlap. Only the exclusion constraint would stop it — and v1.1 did not handle that error (→ HTTP 500). |
| **Extra moving part** | A Redisson VIN lock (5 s lease) duplicated what the VIN exclusion constraint already guarantees, and could expire mid-transaction. Removed. |

### 7.3. Alternatives considered

| Option | Verdict |
| :--- | :--- |
| `SERIALIZABLE` + retry on `40001` | Correct, but retries are noisy and hard to reason about under load. |
| Optimistic: insert, catch `23P01`, try next candidate | Correct, lock-free; more round-trips per booking and more complex code. Good future option if one dealership ever becomes hot. |
| **Advisory lock per dealership + constraints (chosen)** | Simple, correct, no false negatives. A workshop takes tens of bookings per hour, so per-dealership serialization costs nothing; different dealerships proceed fully in parallel. |

---

## 8. Detailed Data Flows

### 8.1. Create appointment

The sequence retains the v2.0 **target design**. Idempotency fast-path/re-check, replay headers and post-commit availability version bumps are **Designed-only**; the current `book(BookingCommand)` implements the catalog/hours/lock/allocation/atomic-outbox path and after-commit decision log. Redis catalog I/O currently occurs inside the booking transaction before the dealership lock, bounded by the short timeouts.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant API as AppointmentController
    participant B as BookingService
    participant R as Redis
    participant DB as PostgreSQL

    C->>API: POST /api/v1/appointments (Idempotency-Key?)
    API->>B: book(command, key)
    B->>R: GET scheduler:idempotency:{key}
    alt hit, same requestHash
        B->>DB: load appointment
        B-->>C: 201 + Idempotent-Replayed: true
    end
    B->>R: dealership / service type (read-through, fail-open)
    B->>B: TimeSlot.of(start, duration) + OperatingHoursPolicy
    B->>DB: BEGIN
    B->>DB: pg_advisory_xact_lock(dealership)
    B->>DB: idempotency re-check → VIN check → free bay → free technician
    alt resource missing
        B->>DB: ROLLBACK
        B-->>C: 409 Problem Details (errorCode)
    else allocated
        B->>DB: INSERT appointment + INSERT outbox_events(PENDING)
        B->>DB: COMMIT
        B->>R: SET idempotency (24h) · INCR availability version
        B-->>C: 201 Created
    end
```

### 8.2. Outbox relay (no DB transaction held across network I/O)

```mermaid
sequenceDiagram
    participant Relay as OutboxRelay
    participant DB as PostgreSQL
    participant K as Kafka
    loop every 500 ms
        Relay->>DB: tx1: UPDATE … SET status='IN_FLIGHT', claim_token=UUID, claimed_at=now(), attempts+1<br/>WHERE id IN (SELECT … PENDING or stale IN_FLIGHT … FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *
        Relay->>K: send(key=appointmentId, value=CloudEvent, headers) — wait for ack (5 s)
        alt ack
            Relay->>DB: tx2: token-fenced status='PUBLISHED', published_at=now()
        else error
            Relay->>DB: tx2: token-fenced status = attempts ≥ 10 ? 'FAILED' : 'PENDING', last_error, next_attempt_at
        end
    end
```

`SKIP LOCKED` is correct *here*: it lets multiple relay instances claim disjoint batches. Rows stuck `IN_FLIGHT` for > 30 s (crashed instance) are reclaimed.

The relay claims one event at a time, drains at most ten per invocation, waits at most five seconds for the send future, and uses producer max-block/request/delivery bounds of 3/3/4 seconds. Retry backoff is 1, 2, 4, 8, 16, then 30 seconds; attempts count claims. Expired final leases become FAILED. A stale worker's token cannot update a replacement claim. Publish-then-crash can still duplicate delivery; producer idempotence does not remove application-level outbox replays. FAILED rows remain for inspection; a replay endpoint/CLI is Designed-only.

### 8.3. Availability read

**Designed-only. No availability endpoint or availability cache is currently registered.**

`GET /dealerships/{id}/availability?date=2030-01-07&serviceTypeId=…` → read version counter → cache hit returns slots; miss loads active bays, qualified technicians and that day's confirmed appointments in 3 queries, runs `SlotFinder` (30-minute grid, inside opening hours, not in the past), caches for 60 s.

### 8.4. Cancel

**Designed-only. No cancellation endpoint or cancelled event is currently registered.**

`POST /appointments/{id}/cancel` → one transaction: `UPDATE … SET status='CANCELLED' WHERE id=? AND status='CONFIRMED'` + outbox `cancelled` event → bump availability version.

---

## 9. API Contract

Base path `/api/v1`. Full contract served at `/swagger-ui.html` and `/v3/api-docs`.

| Method & Path | Success | Purpose |
| :--- | :--- | :--- |
| `POST /appointments` | `201` + `Location` | Book (auto-allocate bay + technician) |
| `GET /appointments/{id}` | `200` | Read appointment |
| `POST /appointments/{id}/cancel` | `200` (proposed) | **Designed-only:** cancel/free resources |
| `GET /dealerships/{id}/availability?serviceTypeId=&date=` | `200` (proposed) | **Designed-only:** free start times |

**Request**

```json
{
  "dealershipId": "11111111-1111-1111-1111-111111111111",
  "customerId": "CUST-90812",
  "vehicleVin": "WBA3A5C51CF256789",
  "serviceTypeId": "44444444-4444-4444-4444-444444444401",
  "desiredStartTime": "2030-01-07T09:00:00Z"
}
```

**Response `201`** — same shape as the event `data` object in §6.4.

**Error model (RFC 9457 Problem Details)**

The following is the v2.0 target error shape. Implemented errors use `type: about:blank`, the enum code as `title`, and an `errorCode` property; custom type URIs and `traceId` are **Designed-only**.

```json
{
  "type": "https://api.keyloop.example/errors/no-qualified-technician-available",
  "title": "No qualified technician available",
  "status": 409,
  "detail": "No active technician with skill EV_HIGH_VOLTAGE_CERT is free between 2030-01-07T09:00:00Z and 2030-01-07T11:00:00Z",
  "instance": "/api/v1/appointments",
  "errorCode": "NO_QUALIFIED_TECHNICIAN_AVAILABLE",
  "traceId": "65f8a9c1e2b34d56a7890c1d2e3f4a5b"
}
```

| `errorCode` | HTTP |
| :--- | :--- |
| `VALIDATION_FAILED` (missing field, bad VIN/UUID, NUL customer ID), `INVALID_START_TIME` | 400 |
| `DEALERSHIP_NOT_FOUND`, `SERVICE_TYPE_NOT_FOUND`, `APPOINTMENT_NOT_FOUND` | 404 |
| `NO_SERVICE_BAY_AVAILABLE`, `NO_QUALIFIED_TECHNICIAN_AVAILABLE`, `VEHICLE_ALREADY_BOOKED` | 409 |
| `START_TIME_IN_PAST`, `OUTSIDE_OPERATING_HOURS` | 422 |
| `APPOINTMENT_ALREADY_CANCELLED`, `IDEMPOTENCY_KEY_REUSED` | **Designed-only** (proposed 409 / 422) |

---

## 10. Technology Stack & Justifications

| Layer | Technology | Justification |
| :--- | :--- | :--- |
| Language | **Java 21 (LTS)** | Records for immutable domain/DTOs. Virtual-thread tuning is Designed-only. |
| Framework | **Spring Boot 4.1** | Current generation (Initializr default); declarative transactions, Actuator, structured logging, OpenTelemetry starter. |
| Data access | **Spring `JdbcClient`** (no JPA) | The core logic *is* the SQL (advisory lock, overlap predicates). Explicit SQL is easier to review and verify than ORM-generated queries. |
| Database | **PostgreSQL 16 + Flyway** | `tstzrange`, `btree_gist` exclusion constraints, advisory locks, `SKIP LOCKED` for the outbox. Versioned migrations + seed data. |
| Cache | **redis:7.4-alpine** (Spring Data Redis / Lettuce) | Catalog cache, TTL 1 h, fail-open. Availability/idempotency replay are Designed-only. |
| Messaging | **apache/kafka:4.3.2 (KRaft)** | Transactional outbox decouples booking from downstream domains; no ZooKeeper. |
| Event UI | **kafbat/kafka-ui:v1.5.0** | Topic messages, keys/headers/body and consumer lag. |
| API docs / mock client | **springdoc-openapi 3.2.0** | Compatible with Boot 4.1; Swagger UI is the mock client. |
| Observability | **Actuator + ECS JSON logs** | Health/readiness and decision/event/cache logs implemented. Prometheus/custom metrics, tracing and Grafana LGTM are Designed-only (§11). |
| Testing | **JUnit Jupiter, AssertJ, Testcontainers (Postgres, Redis, Kafka), Awaitility** | Real infrastructure for concurrency and failure/recovery semantics. |

---

## 11. Observability Strategy

**Logging (Implemented)** — ECS-format JSON to stdout. After-commit confirmations log appointment/dealership/service/resource identifiers; HTTP booking rejections log the stable reason code. Cache bypass and outbox publish/retry logs omit payloads. Kafka's default payload-logging producer listener is replaced. Customer IDs and VINs are not logged. Trace/span correlation is Designed-only.

**Metrics (Designed-only)** — the following custom metrics and `/actuator/prometheus` are not currently exposed:

| Metric | Type | Tags / meaning |
| :--- | :--- | :--- |
| `scheduler.bookings` | Counter | `outcome` = `CONFIRMED`, `REPLAYED`, or the `errorCode` |
| `scheduler.booking.duration` | Timer (p50/p95/p99) | End-to-end booking latency |
| `scheduler.outbox.pending` | Gauge | Unpublished outbox rows — alert if growing (Kafka down) |
| `scheduler.outbox.published` / `scheduler.outbox.failed` | Counter | Relay throughput / dead events |
| `scheduler.cache` | Counter | `cache`, `result=hit|miss|error` |

**Tracing (Designed-only)** — proposed OpenTelemetry/Micrometer correlation persists W3C `traceparent` in the outbox and Kafka headers. The current schema does not have `trace_parent`, no OTLP exporter is configured, and there is no Compose observability profile. A future Grafana LGTM profile would provide Tempo/Prometheus/Loki.

**Event visibility** — Kafka UI on `http://localhost:8081`: topic `keyloop.ownership.appointments.v1` → *Messages* shows publish timestamp, partition/offset, key, headers and payload; *Consumers* shows `notification-service-demo` lag.

**Health (Implemented)** — `/actuator/health` and `/actuator/health/readiness` require PostgreSQL. Redis and Kafka are optional for booking and excluded from aggregate health; an UP response does not prove async delivery. Inspect outbox status and Kafka UI during the demo. Custom backlog/optional-dependency metrics remain Designed-only. Only health/info management endpoints are exposed.

---

## 12. Reliability & Failure Modes

| Failure | Behaviour |
| :--- | :--- |
| Redis down | **Implemented:** cache bypass logged; bookings remain correct via PostgreSQL. Custom cache metrics Designed-only. |
| Kafka down | **Implemented:** booking succeeds; bounded send failures return rows to PENDING/backoff; relay drains after recovery until the 10-attempt limit. Backlog gauge Designed-only. |
| App crashes after commit, before publish | Row is still `PENDING` / stale `IN_FLIGHT` → re-published (at-least-once). |
| Event published, crash before marking `PUBLISHED` | Duplicate delivery; consumers dedupe on CloudEvent `id`. |
| Client times out and retries | **Designed-only replay:** proposed same-key retry returns original appointment. Current behavior may return VIN overlap after a successful earlier commit. |
| Same VIN booked at two dealerships concurrently | VIN exclusion constraint → second gets `409 VEHICLE_ALREADY_BOOKED`. |
| Poison event (fails 10×) | **Implemented:** stored as FAILED for inspection. Failure counter and replay tooling are Designed-only. |

---

## 13. Scalability Path

- **Stateless app** → horizontal scaling behind a load balancer; advisory locks live in Postgres so they work across instances.
- **Write contention** is bounded per dealership; dealerships are independent → natural **sharding key** (`dealership_id`) if a single Postgres ever becomes the bottleneck.
- **Read-heavy availability browsing (Designed-only)** would be absorbed by the Redis availability cache; current Redis usage is catalog-only.
- **Outbox relay** scales out (claims use `SKIP LOCKED`); could be replaced by CDC (Debezium) without changing the event contract.
- **Security (next step):** OAuth2 resource server; dealership-scoped JWT claims enforced in the controller layer; rate limiting at the gateway.
- **Archival (Designed-only):** monthly partitioning needs an explicit strategy for cross-partition VIN/bay/technician exclusion; the current global constraints must not be lost by naively partitioning.

---

## 14. Testing Strategy

The table below retains the v2.0 target test strategy. `TimeSlot`, DST/hours, active/qualified allocation, all implemented HTTP errors, direct database exclusions, adjacent inserts, one/two-resource concurrency, cross-dealership VIN races, transactional rollback, outbox leases/retries/fencing, Kafka key/header/body delivery and broker outage, Redis TTL/hit/corruption/outage, Swagger/health and privacy-safe after-commit logs are **Implemented and tested**. `SlotFinder`, `RequestFingerprint`, cancel, availability and idempotency tests remain **Designed-only**. Verification results are recorded separately so intended coverage is not confused with an executed test.

| Level | What | Examples |
| :--- | :--- | :--- |
| Unit (no Spring) | Pure domain rules | `TimeSlot` overlap incl. back-to-back & containment; `OperatingHoursPolicy` incl. DST (London summer time) and past times; `SlotFinder` grid & busy resources; `RequestFingerprint` stability |
| Integration (Testcontainers) | Real Postgres/Redis/Kafka | happy path allocation & fewest-skills technician choice; each `409`/`422`; cancel frees slot; idempotent replay and key reuse; availability excludes booked slots |
| Concurrency | The core guarantee | 20 threads book the same slot at a 1-bay dealership → exactly 1 × `201`, 19 × `409`, 1 row in DB |
| Messaging | Outbox → Kafka | booking produces a CloudEvent with `key = appointmentId`, `ce_type` header; outbox row becomes `PUBLISHED` |
| API | MockMvc over the full context | status codes, Problem Details shape, `Location` header, validation errors |

---

## 15. Generative AI Collaboration in the Design Phase

**Approach: AI drafts, a second AI cross-examines, I decide and verify.**

1. **Drafting (Gemini).** I gave Gemini the challenge brief and asked it to choose a scenario and produce a design. It chose Scenario A and produced v1.1: Postgres + Redis (Redisson VIN lock, idempotency) + Kafka outbox, with `FOR UPDATE SKIP LOCKED` for allocating bays and technicians.
2. **Adversarial review (second agent, Qoder).** Instead of accepting the polished draft, I had a different AI agent review it against the brief, explicitly asking it to look for correctness bugs, over-engineering and missing requirements. It reported:
   - `SKIP LOCKED` on bay rows causes **false 409s** under concurrency and does not protect the overlap predicate (snapshot race) — the exclusion-constraint error that would result was unhandled.
   - Redis-only idempotency loses the record if the process dies between commit and `SET`.
   - The outbox relay held a DB transaction open during the Kafka send — the very thing the doc claimed to avoid.
   - Missing: test strategy, explicit scope, customer/vehicle ownership assumption, technician allocation fairness, `GET`/cancel endpoints.
3. **My decisions (human ownership).**
   - Verified each claim against PostgreSQL semantics (READ COMMITTED snapshots per statement, row locks vs. predicate locks) before accepting it.
   - Chose *advisory lock per dealership + exclusion constraints* over `SERIALIZABLE` or optimistic retry, after weighing the alternatives in §7.3.
   - **Kept Redis and Kafka**, but re-scoped Redis to a pure accelerator (catalog, availability, idempotency replay) with Postgres as the authority, and added Kafka UI so event timing and payloads are observable during the demo.
   - Removed the Redisson lock; changed the Kafka key to `appointmentId`.
4. **Original verification plan (target coverage).** Every rule in §3 was intended to map to an automated test (§14); the concurrency test against real Postgres is the acceptance gate for the AI-generated allocation code.

5. **Implementation and human review (Codex).** Codex initially delivered a narrower PostgreSQL-backed booking flow and shortened this document, while Redis/Kafka and hours validation were omitted. My review identified that scope drift, the missing opening-hours check and a concurrency fixture that named `Alex Morgan` instead of seeded `Alex Mercer`. I required the implementation to return to the v2.0 architecture: hours first, then a real transactional outbox/Kafka/UI, catalog-only fail-open Redis, Swagger/Actuator, and restoration of this full design. Idempotency/availability/cancellation remain explicitly Designed-only rather than being claimed as delivered.
6. **Verified corrections.** Codex reproduced the 02:00 booking returning 201 and the fixture updating zero rows before fixing them. Subsequent tests verify transaction rollback when outbox insertion fails, lease reclamation/fencing, stable event IDs and bounded retries, real Kafka delivery and outage behavior, Redis fallback/TTL, API docs/health and after-commit decision logs. The review also caught Kafka's default error listener logging event payload excerpts; a regression now requires redacted producer-error logs. The final verification record distinguishes automated checks, local demo observations and remaining design work.
