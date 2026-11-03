# Unified Service Scheduler

Backend MVP for Keyloop Technical Assessment, Scenario A (Ownership). It books a vehicle service appointment and allocates an active service bay and an active technician with the required skill for the full appointment duration.

See [docs/system-design.md](docs/system-design.md) for architecture, data flow, assumptions, concurrency behavior, observability, and the AI collaboration narrative.

## Run locally

Docker Compose builds the Java 21 application image and starts PostgreSQL. A host Java installation is not required.

```bash
docker compose up --build
```

The API is available at `http://localhost:8080`. PostgreSQL is available on `localhost:5432`. Both ports are bound to localhost only. Flyway creates the schema and local demo catalog on application startup.

Swagger UI: `http://localhost:8080/swagger-ui.html`; OpenAPI: `/v3/api-docs`; health: `/actuator/health`.
For a native Java 21 launch, start this project's infrastructure and pass the local database password explicitly:

```bash
docker compose up -d postgres redis kafka kafka-ui
SCHEDULER_DB_PASSWORD=scheduler .tools/mvn21 spring-boot:run
```

`.tools/mvn21` is a local helper on this workstation; with a Java 21 `JAVA_HOME`, use `./mvnw spring-boot:run` instead.

Stop the stack and preserve demo data:

```bash
docker compose down
```

Reset the local database and demo data:

```bash
docker compose down -v
```

## API demo

Create an appointment with a VIN that has not already been booked in the local database:

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

The response is `201 Created`, includes the allocated bay and technician, and returns a `Location` header such as `/api/v1/appointments/<id>`. Use that ID to read the saved appointment:

```bash
curl -i http://localhost:8080/api/v1/appointments/<id>
```

Submitting the same VIN and overlapping time again returns `409 Conflict` with `errorCode: VEHICLE_ALREADY_BOOKED`. If re-running the demo against a database with retained data, change the VIN to another valid 17-character value or reset the local volume.

The seeded dealership ID is `11111111-1111-1111-1111-111111111111`. Seeded service types are `OIL_CHANGE` (`44444444-4444-4444-4444-444444444401`) and `BRAKE_INSPECTION` (`44444444-4444-4444-4444-444444444402`).

## Tests

With Java 21 installed locally:

```bash
./mvnw clean verify
```

To use the Java 21 container and the host Docker engine for Testcontainers:

```bash
docker run --rm \
  -v "$PWD":/workspace \
  -v scheduler-maven-cache:/root/.m2 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  -w /workspace \
  maven:3.9-eclipse-temurin-21 \
  ./mvnw clean verify
```

The tests cover interval boundaries, migrations and seed data, allocation choices, VIN and resource conflicts, API validation and status mapping, retrieval, and concurrent allocation against PostgreSQL 16.

Verified in the Java 21 container: `./mvnw clean verify` completed with 40 tests, 0 failures, and 0 errors. The Compose demo returned `201 Created` for booking, `200 OK` for retrieval, and `409 Conflict` with `VEHICLE_ALREADY_BOOKED` for the overlapping repeat request.

## MVP assumptions and limitations

- Service duration and required technician skill are fixed by each service type.
- Appointments use half-open UTC intervals, so back-to-back bookings are allowed.
- Request timestamps and calculated appointment ends must be no later than `9999-12-31T23:59:59.999999Z`, keeping values within the supported database/runtime range.
- The dealership timezone and operating hours are stored, but opening-hours and DST validation are not implemented in this MVP.
- There is no availability search endpoint, cancellation, rescheduling, authentication, customer/vehicle master-data integration, Redis, or Kafka.
- Demo catalog records and Compose credentials are local-only.

## AI collaboration narrative

Qoder supplied the initial implementation plan and system design. I compared them with the challenge PDF, narrowed the scope to the backend acceptance criteria, and corrected claims that described unbuilt features as implemented. I used Codex to scaffold and implement the Java service, while reviewing the data model, SQL, API errors, and concurrency decisions. I asked for tests before each behavior, watched the migration, interval, API, and concurrency checks fail for the expected missing behavior, and then verified the fixes against PostgreSQL Testcontainers. I also ran the Compose demo and checked that booking, retrieval, and conflict responses matched the documented API. The final source, tests, and README were reviewed against the challenge deliverables.
