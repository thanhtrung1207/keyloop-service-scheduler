# Unified Service Scheduler MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a demonstrable Spring Boot backend that safely creates and retrieves service appointments with a free service bay and qualified technician.

**Architecture:** Spring MVC validates REST requests and delegates to a booking service. PostgreSQL stores catalog and appointments; one transaction-level dealership lock serializes local allocation, and GiST exclusion constraints protect confirmed bay, technician, and VIN intervals. Flyway owns schema and demo seed data.

**Tech Stack:** Java 21, Spring Boot 4.1.2, Spring MVC, Bean Validation, Spring JDBC, PostgreSQL 16, Flyway, JUnit 5, Spring Boot Test, and Testcontainers PostgreSQL. Use a Java 21 Docker build/runtime so the project is runnable despite the host's Java 8 default. Add Swagger UI only if it resolves cleanly with this Spring Boot line; curl examples remain the required demo path.

**Spec:** `docs/superpowers/specs/2026-11-03-scheduler-mvp-design.md`

## Global Constraints

- The attached Keyloop challenge PDF is the requirements source.
- Complete the core booking flow before optional polish; deadline is 2026-11-04.
- Do not add Redis, Kafka, an outbox, a frontend, authentication, or a separate observability stack.
- Use half-open appointment intervals `[start, end)`; adjacent appointments are allowed.
- A booking requires an active bay and an active technician with the service type's required skill for the entire interval.
- PostgreSQL is authoritative; enforce non-overlap for confirmed appointments by VIN, bay, and technician.
- Document implemented and planned capabilities truthfully.

## Review Focus

- Malformed/missing request values must yield a client error, never HTTP 500: cover invalid JSON, missing IDs, malformed timestamp, and invalid VIN in API tests.
- Inactive bays or technicians and technicians without the required skill must never be allocated: cover each in booking tests.
- Adjacent intervals must be accepted while any partial overlap must be rejected: cover exact boundary and overlapping cases in domain/database tests.
- Unknown dealership or service type must return not-found; no available bay or technician must return conflict: cover each API mapping.
- Concurrent requests for the same one-bay/one-technician slot must not create overlapping confirmed appointments: cover with a PostgreSQL integration test when Docker is available.

---

## File Map

```text
pom.xml                                      Maven build and dependencies
Dockerfile                                   Java 21 application image
mvnw, mvnw.cmd, .mvn/wrapper/*              Maven Wrapper
src/main/java/com/keyloop/scheduler/
  ServiceSchedulerApplication.java           Spring Boot entry point
  config/ClockConfig.java                    Injectable UTC clock
  catalog/Dealership.java                    Dealership record
  catalog/ServiceType.java                   Service duration and required skill
  booking/Appointment.java                   Persisted appointment response/domain record
  booking/BookingCommand.java                Validated booking input
  booking/BookingService.java                 Transactional booking and retrieval
  booking/BookingRepository.java              JDBC allocation and appointment SQL
  booking/BookingRejectedException.java       Stable domain rejection reason
  api/AppointmentController.java             REST routes
  api/ApiExceptionHandler.java               Stable Problem Details mapping
src/main/resources/application.yaml          Local datasource and API configuration
src/main/resources/db/migration/V1__schema.sql Schema and exclusion constraints
src/main/resources/db/migration/V2__seed.sql  Small deterministic demo catalog
src/test/java/com/keyloop/scheduler/
  booking/TimeSlotTest.java                   Interval boundary unit tests
  booking/BookingServiceIntegrationTest.java  PostgreSQL-backed booking cases
  booking/ConcurrentBookingTest.java          Concurrent allocation acceptance
  api/AppointmentApiTest.java                 HTTP validation and status mapping
README.md                                     Build/run/test/demo and AI narrative
docs/system-design.md                         Honest MVP/future scope and architecture
```

### Task 1: Scaffold the Spring Boot service

**Files:**
- Create: `pom.xml`, Maven Wrapper, `.gitignore`
- Create: `src/main/java/com/keyloop/scheduler/ServiceSchedulerApplication.java`
- Create: `src/main/resources/application.yaml`

**Interfaces:**
- Produces the runnable `com.keyloop.scheduler.ServiceSchedulerApplication` Spring Boot entry point.

- [x] **Step 1: Scaffold Java 21 / Spring Boot 4.1.2 build**

Add Spring MVC, validation, JDBC, Flyway core + PostgreSQL module, PostgreSQL runtime driver, Spring Boot test, and Testcontainers PostgreSQL/JUnit dependencies. Generate the Maven Wrapper for the selected Maven distribution. Set compiler release to 21.

- [x] **Step 2: Add application configuration and entry point**

Configure PostgreSQL from `SCHEDULER_DB_URL`, `SCHEDULER_DB_USERNAME`, and `SCHEDULER_DB_PASSWORD`, with local defaults matching `compose.yaml` from Task 2. Configure Flyway and a UTC `Clock` bean in `ClockConfig`.

- [x] **Step 3: Compile the scaffold**

Run: `docker run --rm -v "$PWD":/workspace -v scheduler-maven-cache:/root/.m2 -w /workspace maven:3.9-eclipse-temurin-21 ./mvnw -DskipTests compile`
Expected: PASS using Java 21 without installing software on the host. The database-backed context is exercised in Task 2 after PostgreSQL exists.

### Task 2: Add PostgreSQL schema, seed data, and local Compose

**Files:**
- Create: `compose.yaml`
- Create: `Dockerfile`
- Create: `src/main/resources/db/migration/V1__schema.sql`
- Create: `src/main/resources/db/migration/V2__seed.sql`
- Test: `src/test/java/com/keyloop/scheduler/booking/SchemaIntegrationTest.java`

**Interfaces:**
- Produces catalog tables `dealerships`, `service_types`, `service_bays`, `technicians`, `technician_skills`, and `appointments`.
- Seed IDs are fixed UUIDs documented in README and used by curl examples.

- [x] **Step 1: Write the schema integration test**

Create `SchemaIntegrationTest` that starts PostgreSQL Testcontainers, applies Flyway migrations, and asserts catalog table and seed counts.

- [x] **Step 2: Add PostgreSQL schema migration**

Use UUID primary keys; store dealership IANA timezone and daily opening/closing times, service duration and required skill, resource status, technician skill rows, and appointments with customer ID, VIN, interval, assigned resource IDs, status, and timestamps. Add `btree_gist` and confirmed-only exclusion constraints for bay, technician, and VIN over `tstzrange(start_time, end_time, '[)')`.

- [x] **Step 3: Add deterministic local seed data**

Seed one dealership, two service types, two active bays, and active technicians with different skills. Ensure one service request can succeed and the catalog also demonstrates no-qualified-technician cases.

- [x] **Step 4: Add localhost-only PostgreSQL and application Compose services**

Publish PostgreSQL only on `127.0.0.1:5432`, use a named local volume and a readiness healthcheck. Build the application as a Java 21 runtime image and expose its API only on `127.0.0.1:8080`; configure the database URL on the Compose network and wait for PostgreSQL health. Keep credentials local/demo-only and overridable by environment.

- [x] **Step 5: Verify migration and seed load**

Run: `./mvnw -Dtest=SchemaIntegrationTest test`
Expected: Testcontainers starts PostgreSQL, Flyway applies both migrations, and the expected seed rows can be queried.

### Task 3: Implement interval policy and JDBC allocation queries

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/booking/TimeSlot.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/catalog/Dealership.java`
- Create: `src/main/java/com/keyloop/scheduler/catalog/ServiceType.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/TimeSlotTest.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/BookingRepositoryIntegrationTest.java`

**Interfaces:**
- `TimeSlot(Instant start, Instant end)` rejects null or non-positive intervals; `overlaps(TimeSlot other)` uses `start < other.end && end > other.start`.
- `Dealership(UUID id, String name, ZoneId timezone, LocalTime openTime, LocalTime closeTime)`.
- `ServiceType(UUID id, String code, String name, int durationMinutes, String requiredSkill)`.
- `BookingRepository.findDealership(UUID)` and `findServiceType(UUID)` return `Optional` values; `lockDealership(UUID)` obtains a transaction-scoped lock; `hasVinOverlap(String, TimeSlot)` checks a confirmed VIN overlap; `findFreeBay(UUID, TimeSlot)` and `findFreeTechnician(UUID, String, TimeSlot)` return optional allocation records ordered as specified below.

- [x] **Step 1: Write interval boundary tests**

Test overlapping intervals, equal endpoints as adjacent/non-overlapping, containment, and invalid zero/negative duration.

- [x] **Step 2: Implement `TimeSlot`**

Use `Instant` and the half-open overlap predicate from the approved design.

- [x] **Step 3: Implement JDBC catalog and allocation SQL**

Use `JdbcClient` with bound parameters. Acquire `pg_advisory_xact_lock(hashtextextended(CAST(:dealershipId AS text), 0))`. Select the first active free bay by `bay_code`; select an active free technician joined to the required skill, ordered by skill count, name, and ID. All overlap queries use half-open comparisons.

- [x] **Step 4: Verify unit and repository integration tests**

Run: `./mvnw -Dtest=TimeSlotTest,BookingRepositoryIntegrationTest test`
Expected: PASS, including adjacent resource use and exclusion-constraint rejection on overlap.

### Task 4: Implement transactional booking and retrieval

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingCommand.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/Appointment.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingRejectedException.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/RejectionReason.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingService.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/BookingServiceIntegrationTest.java`

**Interfaces:**
- `BookingCommand(UUID dealershipId, String customerId, String vehicleVin, UUID serviceTypeId, Instant desiredStartTime)`.
- `BookingService.book(BookingCommand command): Appointment`.
- `BookingService.get(UUID appointmentId): Appointment`.
- Stable rejection reasons: `DEALERSHIP_NOT_FOUND`, `SERVICE_TYPE_NOT_FOUND`, `START_TIME_IN_PAST`, `INVALID_VIN`, `VEHICLE_ALREADY_BOOKED`, `NO_SERVICE_BAY_AVAILABLE`, `NO_QUALIFIED_TECHNICIAN_AVAILABLE`, `APPOINTMENT_NOT_FOUND`.

- [x] **Step 1: Write integration tests for booking outcomes**

Cover successful persistence and assignment, unknown catalog IDs, invalid VIN, past start, VIN overlap, no free bay, and no qualified technician. Assert end time equals start plus service duration.

- [x] **Step 2: Implement booking transaction**

Load catalog metadata, derive the full `TimeSlot`, validate VIN and past start, begin one transaction, take the dealership lock, check VIN overlap, allocate bay and technician, and insert the appointment. Translate PostgreSQL exclusion violations into the appropriate domain conflict so a race cannot become HTTP 500. Add dealership-local opening-hours/DST validation only after the core acceptance tests pass.

- [x] **Step 3: Implement appointment retrieval**

Load appointment and its dealership, service, bay, and technician display fields; return `APPOINTMENT_NOT_FOUND` when absent.

- [x] **Step 4: Run booking integration tests**

Run: `./mvnw -Dtest=BookingServiceIntegrationTest test`
Expected: PASS against PostgreSQL Testcontainers.

### Task 5: Expose REST endpoints and stable errors

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/api/AppointmentController.java`
- Create: `src/main/java/com/keyloop/scheduler/api/ApiExceptionHandler.java`
- Test: `src/test/java/com/keyloop/scheduler/api/AppointmentApiTest.java`

**Interfaces:**
- `POST /api/v1/appointments` returns `201 Created` and the saved `Appointment`.
- `GET /api/v1/appointments/{id}` returns `200 OK` or `404 Not Found`.
- Invalid request returns `400 Bad Request`; allocation/overlap conflicts return `409 Conflict`; past requests return `422 Unprocessable Entity`.

- [x] **Step 1: Write MockMvc API tests**

Test successful create and retrieval, invalid JSON, missing fields, malformed timestamp, malformed VIN, unknown appointment ID, past start, and conflict status mapping. Assert RFC 9457 response includes stable `errorCode`.

- [x] **Step 2: Implement validated controller DTO and routes**

Apply Bean Validation to IDs, nonblank customer ID, VIN pattern excluding I/O/Q, and required timestamp. Controller delegates to `BookingService` only.

- [x] **Step 3: Implement centralized API exception mapping**

Return Spring `ProblemDetail` with status, title, stable `errorCode`, and field-validation detail. Do not return stack traces or database messages to clients. If optional opening-hours validation is added, map it to 422.

- [x] **Step 4: Run API tests**

Run: `./mvnw -Dtest=AppointmentApiTest test`
Expected: PASS; no invalid input or domain conflict produces HTTP 500.

### Task 6: Prove concurrent booking safety

**Files:**
- Create: `src/test/java/com/keyloop/scheduler/booking/ConcurrentBookingTest.java`

**Interfaces:**
- Uses the real PostgreSQL integration-test context and `BookingService.book`.

- [x] **Step 1: Add concurrent resource-capacity acceptance test**

Submit 12 simultaneous requests with distinct VINs for the same interval to a dealership with two available bays and two qualified technicians. Drop the two resource exclusion constraints in this isolated test database (they are separately covered in Task 3), and use a short insert trigger delay to force the allocation race. Assert exactly two appointments are created on distinct bays and technicians, remaining requests are domain conflicts, and no request fails with an unhandled exception.

- [x] **Step 2: Run the concurrency test**

Run: `./mvnw -Dtest=ConcurrentBookingTest test`
Expected: PASS with exactly two confirmed appointments using both resource pairs.

### Task 7: Complete delivery documentation and run acceptance checks

**Files:**
- Create: `README.md`
- Modify: `docs/system-design.md`
- Modify: `docs/superpowers/specs/2026-11-03-scheduler-mvp-design.md` only if implementation assumptions materially change

- [x] **Step 1: Update system design scope/statuses**

Mark only the implemented MVP features complete after observing code and test evidence. Mark Redis, Kafka/outbox, full availability search, auth, and advanced calendars as future/out of scope. Preserve architecture diagram, component roles, data flow, technology justifications, observability approach, and AI design narrative required by the PDF.

- [x] **Step 2: Write README run and demo instructions**

Document Java 21, Docker Compose PostgreSQL startup, `./mvnw test`, application startup, API endpoints, seed IDs, success curl, conflict curl, assumptions, limitations, and an AI collaboration narrative that describes actual review/debug/verification rather than planned evidence.

- [x] **Step 3: Run full automated acceptance and whitespace check**

Run: `docker run --rm -v "$PWD":/workspace -v scheduler-maven-cache:/root/.m2 -v /var/run/docker.sock:/var/run/docker.sock -w /workspace maven:3.9-eclipse-temurin-21 ./mvnw clean verify`
Expected: all unit, database/API, and concurrency tests pass using the mounted local Docker engine.

- [x] **Step 4: Run the local demo**

Start PostgreSQL and the application using `docker compose up --build`. Create and retrieve an appointment; send an overlapping request and confirm a client conflict response. Record actual command outcomes in the README only if useful and accurate.

## Self-review

- Spec coverage: create and retrieve REST API, persistent catalog/appointment storage, full-interval bay and qualified technician allocation, VIN/resource overlap protection, tests, local run/demo, and honest system design/AI narrative are covered by Tasks 1–7.
- Scope: Redis, Kafka, frontend, auth, and separate monitoring are explicitly omitted per the approved MVP.
- Execution: tasks depend on the interfaces established in prior tasks and each has a focused test command.
- Deadline risk: if Testcontainers cannot reach the mounted Docker socket, keep unit tests runnable and validate integration/manual acceptance against the local Compose PostgreSQL with a documented test profile; report any unavailable check precisely rather than claiming it passed.
