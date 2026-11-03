# Review completion design

The user's explicit review response on 2026-11-03 supersedes the earlier MVP narrowing. The authoritative architectural reference is `docs/system-design.md` from commit `4069e20` (v2.0). This supplement records the approved implementation scope; it does not claim that the earlier MVP scope was selected by the user.

## Approved scope and order

1. Validate the full appointment interval against the dealership's local opening hours. Resolve boundaries in the IANA zone, reject nonexistent/ambiguous opening boundaries, permit ending exactly at close, reject intervals crossing the local closing day. Return 422 `OUTSIDE_OPERATING_HOURS`. Fix the concurrency fixture to use Alex Mercer's stable UUID and assert that exactly one oil-qualified active technician remains.
2. Insert a confirmed CloudEvent into `outbox_events` in the appointment transaction. Relay every 500 ms: short claim transaction with `FOR UPDATE SKIP LOCKED`, publish outside a database transaction, then short fenced completion transaction. Reclaim leases after 30 seconds. Retry up to 10 attempts, then FAILED. Topic `keyloop.ownership.appointments.v1`, 3 partitions, key appointmentId, headers ce_type and ce_id. CloudEvents JSON follows v2.0 §6.4, including nested service type/allocation objects and null cancelledAt. Producer acks=all and idempotence enabled. A demo consumer in group `notification-service-demo` logs event identifiers without customer or VIN values. Kafka unavailability cannot fail booking.
3. Redis caches dealership and service type only, TTL 1 hour, read-through, fail-open. No Redis lock or correctness dependency.
4. Add Swagger UI, actuator health, and booking decision logs without customer/VIN data. Log confirmed decisions after transaction commit to avoid reporting rolled-back bookings as confirmed.
5. Restore all 15 sections of v2.0 and its Gemini → Qoder → human decisions narrative; append actual Codex implementation/review evidence. Mark idempotency, cancellation, availability cache, distributed tracing and remaining observability extensions Designed-only wherever referenced.

## Runtime and isolation

- Java 21 / Spring Boot 4.1.2 / PostgreSQL 16 remain in place.
- Compose images: `apache/kafka:4.3.2`, `kafbat/kafka-ui:v1.5.0`, `redis:7.4-alpine`.
- Every published host port is localhost-only. Redis host 6380, Kafka UI 8081; Kafka uses host 29092 and an internal listener. Existing unrelated projects and global Java/Maven settings are untouched.
- Existing tests run without external Kafka/Redis by disabling the relay/cache; dedicated infrastructure tests enable real Kafka/Redis containers.
- Idempotency and cancellation remain Designed-only; existing CONFIRMED-only schema semantics are retained. Future cancellation needs filtered exclusions and status-aware overlap queries.
- Run `./mvnw clean verify` after each stage; infrastructure tests use real PostgreSQL/Kafka/Redis Testcontainers.

## Implementation choices

The user selected a real integration over infrastructure-only scaffolding. PostgreSQL remains authoritative. Outbox claim tokens fence stale workers: the token must match on completion, even after lease reclamation; duplicate Kafka deliveries remain possible and share the same CloudEvent id. The demo consumer does not promise durable deduplication. Failed events remain stored for inspection and future manual replay tooling.
