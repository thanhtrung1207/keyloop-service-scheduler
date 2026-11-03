# Unified Service Scheduler

Keyloop Technical Assessment, Scenario A (Ownership). The backend books a vehicle service appointment, allocates an active bay and a qualified active technician for the complete interval, and publishes a confirmed CloudEvent through a PostgreSQL transactional outbox.

See [system design](docs/system-design.md) for all 15 architectural sections, tradeoffs, concurrency analysis, failure modes, future designs and AI collaboration. See [verification](docs/verification.md) for observed checks.

## Run the complete local demo

Prerequisites: Docker with Compose. Docker builds the Java 21 application; a host JDK is optional.

```bash
docker compose up --build -d
curl --fail http://localhost:8080/actuator/health
```

| Component | Local address |
| --- | --- |
| Booking API | http://localhost:8080/api/v1/appointments |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| OpenAPI JSON | http://localhost:8080/v3/api-docs |
| Health / database readiness | http://localhost:8080/actuator/health and /actuator/health/readiness |
| Kafka UI | http://localhost:8081 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6380 |
| Kafka host listener | localhost:29092 |

Every published host port binds to `127.0.0.1`. Compose uses `apache/kafka:4.3.2` (KRaft), `kafbat/kafka-ui:v1.5.0`, `redis:7.4-alpine`, and PostgreSQL 16. Only PostgreSQL is required for booking; broker/cache outages are handled by the outbox and database fallback.

If 8080 is already in use, launch with `SCHEDULER_HTTP_PORT=18080 docker compose up --build -d` and use API/Swagger/health URLs on port 18080. Kafka UI keeps port 8081.

Compose supplies local demo database credentials (`scheduler` / `scheduler`). Optional overrides are shown in `.env.example`; copy it to `.env` and edit for a new local database. Existing PostgreSQL volumes retain the credentials with which they were initialized. The application's datasource password has no hardcoded fallback; native launches must supply it explicitly.

Stop services while retaining local data:

```bash
docker compose down
```

PostgreSQL and Kafka have named data volumes. Redis is an expendable cache. To reset demo data, intentionally remove this project's volumes with `docker compose down -v`; this deletes existing local appointments and broker data.

## Native Java 21 / IntelliJ

Start the infrastructure, select Java 21 as the project's SDK and Maven importer/runner JDK, then run the application with an explicit password:

```bash
docker compose up -d postgres redis kafka kafka-ui
SCHEDULER_DB_PASSWORD=scheduler ./mvnw spring-boot:run
```

On this workstation a project-local helper is also available:

```bash
SCHEDULER_DB_PASSWORD=scheduler .tools/mvn21 spring-boot:run
```

`.tools` is local-only and is not included in Git. For a machine with an unrelated global Maven mirror, use the project's own IntelliJ Maven settings override; no other repository's SDK/settings need changing. Native app binding defaults to localhost; Compose sets the container binding separately.

## API and event demo

Seed dealership: `11111111-1111-1111-1111-111111111111`, zone `Europe/London`, daily hours 08:00–18:00. Service types: oil change (`44444444-4444-4444-4444-444444444401`, 60 minutes, OIL_SERVICE) and brake inspection (`44444444-4444-4444-4444-444444444402`, 90 minutes, BRAKE_SERVICE).

Use Swagger **Try it out**, or create a booking:

```bash
curl -i http://localhost:8080/api/v1/appointments \
  -H 'Content-Type: application/json' \
  -d '{
    "dealershipId": "11111111-1111-1111-1111-111111111111",
    "customerId": "CUST-DEMO-001",
    "vehicleVin": "WBA3A5C51CF256789",
    "serviceTypeId": "44444444-4444-4444-4444-444444444401",
    "desiredStartTime": "2030-01-07T09:00:00Z"
  }'
```

Expected: `201 Created`, allocated bay/technician, calculated end, and a `Location: /api/v1/appointments/<id>` header.

```bash
curl -i http://localhost:8080/api/v1/appointments/<id>
```

Expected: `200 OK`. Repeat the overlapping VIN request → `409 VEHICLE_ALREADY_BOOKED`. Change the start to `2030-01-07T02:00:00Z` → `422 OUTSIDE_OPERATING_HOURS`. Use a fresh VIN or interval when repeating a demo against retained data.

In Kafka UI, open cluster **keyloop-local**, topic **keyloop.ownership.appointments.v1**, then **Messages**. Verify 3 partitions, key = appointment UUID, headers `ce_type` / `ce_id`, and the full CloudEvents JSON. The `notification-service-demo` consumer group logs event identifiers. Check delivery state in PostgreSQL:

```bash
docker compose exec postgres psql -U scheduler -d scheduler \
  -c 'select id, aggregate_id, status, attempts, published_at from outbox_events order by created_at desc limit 5;'
```

Successful delivery becomes `PUBLISHED`. A broker outage leaves durable `PENDING` events with backoff; after 10 attempts an event becomes `FAILED` for inspection. Replays keep the same CloudEvent id. The demo consumer only logs and does not implement durable business-side deduplication.

Redis contains only `scheduler:catalog:dealership:<id>` and `scheduler:catalog:service-type:<id>` JSON, each with a 1-hour TTL:

```bash
docker compose exec redis redis-cli TTL scheduler:catalog:dealership:11111111-1111-1111-1111-111111111111
```

## Tests

With a Java 21 `JAVA_HOME` and Docker available:

```bash
./mvnw clean verify
```

For the Java 21 container on this Mac's Docker engine:

```bash
docker run --rm \
  -v "$PWD":/workspace \
  -v scheduler-maven-cache:/root/.m2 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  -w /workspace maven:3.9-eclipse-temurin-21 \
  ./mvnw clean verify
```

The latest completed stage ran **73 tests, 0 failures/errors/skips**. Tests cover hours and DST, overlap boundaries, active/qualified resources, concurrency, transactional outbox rollback/leases/fencing/retries, real Kafka delivery/outage, Redis TTL/hit/corruption/outage, API validation, Swagger/health and redacted after-commit decision logs. Dedicated tests start isolated PostgreSQL/Kafka/Redis containers; other tests disable async delivery/cache to avoid requiring an external stack.

## Assumptions and remaining design work

- Service duration and required skill are fixed per service type. Time intervals are half-open `[start, end)`; adjacent bookings are valid.
- The entire elapsed interval must fit one dealership local opening day. UTC instants avoid ambiguous client-local timestamps; ambiguous/nonexistent opening boundaries are rejected explicitly.
- API timestamps and calculated ends are supported through `9999-12-31T23:59:59.999999Z` and persisted at microsecond precision.
- PostgreSQL advisory locks and exclusion constraints own correctness. Redis never supplies a lock or determines resource availability.
- Catalog changes can remain cached for up to 1 hour. A future catalog administration flow needs targeted invalidation.
- Idempotency, cancellation, availability search/cache, shifts/holidays, authentication, master-data integration, distributed tracing and custom Prometheus/Grafana metrics are **Designed-only**. Current schema only permits CONFIRMED appointments; cancellation needs filtered constraints and status-aware queries.
- Outbox delivery is at-least-once. Health/readiness reflects the required database, while event backlog and Kafka UI are inspected separately. Durable downstream deduplication, manual replay tooling and per-aggregate ordering for future cancellation are further work.

## AI collaboration narrative

Gemini drafted v1.1. Qoder cross-reviewed its `SKIP LOCKED` allocation, snapshot-race risk, Redis-only idempotency and transaction-held-during-Kafka-send design. I chose PostgreSQL dealership advisory locks plus exclusion constraints, kept Redis as an accelerator, and retained Kafka/outbox plus Kafka UI for observable event delivery.

Codex implemented and tested the booking service, initially narrowing the scope and shortening the design document. My review identified the missing integrations, hours validation and incorrect technician fixture. I required the implementation to return to the agreed v2.0 scope. Codex then reproduced the defects, added real infrastructure tests, implemented the integrations and restored the architectural analysis. The original narrative and the implementation/review corrections remain in [system-design.md §15](docs/system-design.md#15-generative-ai-collaboration-in-the-design-phase).
