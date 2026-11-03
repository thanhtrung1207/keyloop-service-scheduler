# Unified Service Scheduler MVP Design

## Intent

Deliver a working backend for Keyloop Scenario A by the interview deadline on 2026-11-04. The backend accepts an appointment request for a customer, vehicle, service type, dealership, and desired start time; allocates an available service bay and a qualified technician for the full service duration; and persists the confirmed appointment.

The attached Keyloop challenge PDF is the requirements source. This is a one-day delivery, so the implementation prioritizes demonstrable correctness of the required booking flow and a clear README/demo.

## Goals

- Provide a REST API to create and retrieve appointments.
- Persist dealerships, service bays, technicians and skills, service types, and appointments in PostgreSQL.
- Allocate only active bays and active technicians with the required skill.
- Reject a booking if the vehicle, bay, or technician has an overlapping confirmed appointment during any part of the requested duration.
- Associate the confirmed appointment with the customer, VIN, assigned technician, and service bay.
- Include automated tests for allocation, overlap rules, and the API's successful and rejected booking paths.
- Provide local run instructions and a reproducible request/demo in `README.md`.
- Keep the System Design Document honest about what is implemented versus planned.

## Non-goals for the deadline

- Redis caching, Kafka, a transactional outbox, Kafka UI, and downstream consumers.
- A full availability calendar endpoint, rescheduling, shifts, holidays, or partial-day closures.
- Authentication/authorization, multi-tenant access control, deployment, Grafana, and distributed tracing.
- Building a frontend; the API contract and curl/Swagger are the client demonstration.

## Constraints and assumptions

- Use the existing Qoder direction: Java 21, Spring Boot, Spring JDBC, and PostgreSQL. Do not introduce a new language or persistence stack.
- Service types have a fixed duration and one required technician skill.
- Appointments use half-open intervals `[start, end)`, so adjacent bookings are allowed.
- Request timestamps are ISO-8601 instants. Dealership operating hours and timezone validation follow the existing system-design assumptions if time allows; core acceptance is resource availability for the full duration.
- A vehicle cannot have overlapping confirmed appointments, even at different dealerships.
- No authentication is required by the challenge.
- Seed a small local catalog for repeatable manual demonstration; it is not production data.

## Approaches considered

1. **Full Qoder plan:** PostgreSQL, Redis, Kafka/outbox, availability caching, observability stack, and extensive integration scenarios. This offers more infrastructure depth but adds setup and failure modes that are not required to demonstrate the core challenge within one day.
2. **Focused backend MVP (selected):** PostgreSQL-backed REST API, deterministic bay/technician allocation, database overlap protection, core tests, OpenAPI or curl demo, and README. This directly satisfies the required backend criteria and leaves time to verify the booking flow.

The user selected the focused backend MVP.

## Proposed architecture

- **REST API:** Spring MVC controllers validate create/read requests and return clear success and conflict responses.
- **Booking service:** Loads dealership and service metadata, checks business rules, chooses an eligible bay and technician, and coordinates one database transaction.
- **PostgreSQL:** Stores the catalog and appointment records. A transaction-level dealership lock serializes competing allocations at one dealership; exclusion constraints protect bay, technician, and VIN intervals as a final correctness boundary.
- **Client harness:** Swagger UI if dependency setup remains straightforward; otherwise README curl examples provide the demo.
- **Observability:** Structured application logs with appointment identifiers and rejection reasons, plus a health endpoint if actuator setup is straightforward. No separate monitoring stack in this deadline scope.

## Core data and flow

1. `POST /api/v1/appointments` receives dealership ID, customer ID, VIN, service type ID, and desired start time.
2. Validate request shape, catalog references, VIN format, start time, and service duration.
3. Start a transaction and acquire the dealership-scoped lock.
4. Reject an overlapping confirmed appointment for the VIN.
5. Select an active, free bay and an active, free technician with the service type's required skill for the entire interval.
6. Insert one confirmed appointment with the calculated end time and allocated resource IDs; commit.
7. Return `201 Created` with the appointment ID, interval, bay, and technician. Map resource/vehicle conflicts to `409 Conflict` and invalid requests to `400` or `422` consistently.
8. `GET /api/v1/appointments/{id}` returns the saved appointment for demonstration.

## Tests and acceptance

- Unit tests cover interval overlap, adjacency, and service-duration end-time calculation.
- Database/API integration tests cover successful allocation, no bay, no qualified technician, VIN overlap, resource overlap, and retrieval.
- A concurrency test should verify that competing requests cannot create overlapping appointments for the same single resource, if the local build/test environment permits it within the deadline.
- Final manual acceptance: start the documented local stack, create a booking, retrieve it, and demonstrate a conflicting request returning a client error rather than a server error.

## Deliverables

- Working Java backend and PostgreSQL schema/seed data.
- Updated `docs/system-design.md` whose status labels distinguish implemented MVP from planned extensions.
- README with build/run/test commands, API examples, assumptions, and an honest AI collaboration narrative describing how generated work was reviewed and verified.
- Automated tests for the required business rules.

## Builder calls

- Keep the selected Java/Spring/PostgreSQL stack from Qoder's documents.
- Use database constraints in addition to application checks so concurrent bookings remain safe.
- Defer optional infrastructure until all core acceptance items run successfully.
