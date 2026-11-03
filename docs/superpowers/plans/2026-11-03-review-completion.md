# Review Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the explicitly approved Redis/Kafka integration and review fixes without affecting unrelated projects.

**Architecture:** PostgreSQL owns booking correctness and a transactional outbox. Short leased claims feed Kafka outside database transactions. Redis only accelerates catalog reads; a local Docker stack provides the demo.

**Tech Stack:** Java 21, Spring Boot 4.1.2, Spring JDBC/Data Redis/Kafka, PostgreSQL 16, springdoc 3.2.0, Actuator, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-11-03-review-completion-design.md`; v2.0 design at `4069e20:docs/system-design.md`.

## Global Constraints

- User order: hours validation -> outbox/Kafka/UI -> Redis -> Swagger/actuator -> docs.
- Kafka image apache/kafka:4.3.2; UI kafbat/kafka-ui:v1.5.0; Redis redis:7.4-alpine; localhost-only host ports, Redis 6380, UI 8081.
- Topic keyloop.ownership.appointments.v1, 3 partitions; key appointmentId; headers ce_type, ce_id; CloudEvents v2.0 §6.4; producer acks=all, enable.idempotence=true.
- Relay ~500 ms, 30-second claim timeout, 10-attempt maximum; no transaction held during Kafka send. Kafka outage cannot fail booking.
- Redis catalog-only, 1-hour TTL, fail-open; no distributed locking. Preserve original design depth and AI narrative, with honest status labels.
- Every task finishes with `./mvnw clean verify` (inside the existing Java 21 Maven container with Docker socket and TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal). Capture output in the plan workspace.

## Review Focus

- DST gaps/overlaps and exact opening/closing boundaries: reject invalid windows, accept exact boundaries; Task 1 unit/API regressions.
- Appointment insert and outbox atomicity: force a failed outbox insert and assert appointment rollback; Task 2.
- Kafka outage and stale worker reclamation: durable pending rows, fenced completion and stable event ids, bounded retries; Task 2 real DB tests.
- Redis error/corrupt cached data: database fallback and successful booking; Task 3.
- Swagger health and logs must reflect actual contracts and avoid customer/VIN data; Task 4 API tests and documentation review.

---

### Task 1: Operating hours and truthful concurrency fixture

**Files:** Create `booking/OperatingHoursPolicy.java`, test `booking/OperatingHoursPolicyTest.java`; modify BookingService, RejectionReason, ApiExceptionHandler, BookingServiceIntegrationTest, AppointmentApiTest, ConcurrentBookingTest.
**Interfaces:** Consumes Dealership and TimeSlot. Produces `OperatingHoursPolicy.validate(Dealership, TimeSlot)` and OUTSIDE_OPERATING_HOURS → HTTP 422.

- [ ] Write failing unit and API cases: London 02:00 rejected; 08:00 start / 18:00 end accepted; end past close, next-day interval, London summer conversion and ambiguous/nonexistent boundaries rejected appropriately.
- [ ] Reproduce missing hours validation and the incorrect concurrency fixture (assert update count and eligible technician count).
- [ ] Implement boundary instants using timezone valid offsets; reject ambiguous/nonexistent opening/closing boundaries. Reset bay status between service tests. Fix technician by UUID with affected-row assertion.
- [ ] Run `./mvnw clean verify`; Expected: all pass. Commit `fix: enforce dealership operating hours`.

### Task 2: Transactional outbox, Kafka delivery, and Kafka UI

**Files:** Add migration V3, `events/AppointmentConfirmedEvent.java`, `events/OutboxRepository.java`, `events/OutboxRelay.java`, `events/AppointmentEventsLoggingConsumer.java`, `config/MessagingConfig.java`; modify BookingService, pom.xml, application.yaml, compose.yaml. Add OutboxIntegrationTest and KafkaOutboxIntegrationTest.
**Interfaces:** Consumes Appointment, ServiceType, Clock, JdbcClient. Produces `OutboxRepository.enqueue(Appointment, ServiceType)`, `claim(int)` → leased rows, fenced markPublished/markFailed; Relay `publishPending()` performs no network I/O in DB transaction.

- [ ] Add failing booking test asserting one durable CloudEvent outbox row; atomic rollback case; real DB lease reclaim/fencing/retry exhaustion tests; Testcontainers Kafka test consuming key/headers/body and observing PUBLISHED.
- [ ] Add Boot Kafka/Testcontainers Kafka dependencies; implement migration, event JSON and enqueue inside booking transaction. Implement leased claims in TransactionTemplate and publish after claim returns.
- [ ] Implement mark/retry with token fencing, attempts increment on claim, expired IN_FLIGHT reclaim at 30s, FAILED at 10. Producer send timeout below lease; bounded batch prevents claim timeout before processing (claim 1 per iteration).
- [ ] Configure topic, producer, demo consumer, conditional scheduling, Docker Kafka/Kafka UI with independent internal/external listeners and localhost-only ports.
- [ ] Test Kafka outage: booking succeeds with PENDING row; failed send returns PENDING; existing event id preserved on retry. Verify publisher observes no active DB transaction during send.
- [ ] Run `./mvnw clean verify`; Expected: all pass. Run `docker compose config --quiet`; Expected: exit 0. Commit `feat: publish booking events with transactional outbox`.

### Task 3: Fail-open Redis catalog cache

**Files:** Add `catalog/CatalogService.java`, modify BookingService, pom.xml, application.yaml, compose.yaml. Add CatalogCacheIntegrationTest and RedisUnavailableBookingIntegrationTest.
**Interfaces:** Consumes BookingRepository metadata reads, StringRedisTemplate, JSON mapper. Produces `CatalogService.findDealership(UUID)` and `findServiceType(UUID)`; no negative caching; TTL Duration.ofHours(1).

- [ ] Write tests for cached metadata hit after DB source changes, 1-hour TTL, independent dealership/service keys, invalid cache JSON fallback, and a refused Redis connection while booking succeeds.
- [ ] Implement optional cache with short connect/read timeouts, fail-open reads and writes; all allocation/overlap SQL continues to query PostgreSQL.
- [ ] Add Redis Compose at 127.0.0.1:6380:6379 and app Redis host environment. Disable cache in unrelated existing tests to avoid shared external Redis.
- [ ] Run `./mvnw clean verify`; Expected: all pass. Commit `feat: cache catalog metadata in Redis with database fallback`.

### Task 4: Swagger, health, decision logging, local secrets configuration

**Files:** Modify pom.xml, application.yaml, Compose, README; add API metadata/config and endpoint tests; booking logs emitted after commit.
**Interfaces:** Exposes `/swagger-ui.html`, `/v3/api-docs`, `/actuator/health`; logs decision reason and appointment/resource identifiers without customerId/VIN.

- [ ] Write endpoint tests for generated appointment OpenAPI routes and UP health with real Postgres; assert confirmed log only after commit and rejected booking emits reason without sensitive payload.
- [ ] Add springdoc 3.2.0 and Actuator. Expose health/info only; use database as required health dependency and optional Redis fail-open semantics. Add after-commit confirmation log and rejection logging.
- [ ] Remove hardcoded datasource password fallback from application.yaml, keep explicit local-only Compose defaults/environment example; document native launch environment.
- [ ] Run `./mvnw clean verify`; Expected: all pass. Commit `feat: add API documentation and booking observability`.

### Task 5: Restore v2.0 design and demonstrate delivered behavior

**Files:** Restore docs/system-design.md from 4069e20, update README and superseded MVP design claim, add verification record.
**Interfaces:** Consumes actual behavior and test reports from Tasks 1–4; produces honest Implemented/Designed-only status for every section.

- [ ] Restore all 15 sections, alternatives, SKIP LOCKED explanation, failure modes, scalability, original AI narrative. Preserve future-design snippets with explicit Designed-only labels; align actual DB identifiers/limits/event fields.
- [ ] Append Codex implementation and human review corrections to the Gemini/Qoder/human narrative; remove unsupported assertion that user selected dropping Redis/Kafka.
- [ ] Run `./mvnw clean verify`; Expected: all pass. Start only this project's Compose services, verify health/Swagger, booking/GET/outside-hours, Redis hit and Kafka UI/event/outbox state; preserve pre-existing PostgreSQL service/data on cleanup.
- [ ] Commit `docs: restore complete system design and verified delivery narrative`.
- [ ] Dispatch one fresh whole-branch review per executing-plans. Fix important findings once with focused regressions and a full suite; preserve worktree for user review.
