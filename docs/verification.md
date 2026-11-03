# Verification record

This records observed results, separately from future-facing design claims. Commands ran inside the Java 21 Maven container described in README, with the Docker socket available to Testcontainers. No global Java/Maven settings were changed for this implementation.

| Completed stage | Command | Observed result |
| --- | --- | --- |
| Hours / concurrency fixture | `./mvnw clean verify` | 55 tests, zero failures/errors/skips |
| Transactional outbox / Kafka | `./mvnw clean verify` | 64 tests, zero failures/errors/skips |
| Redis catalog cache | `./mvnw clean verify` | 70 tests, zero failures/errors/skips |
| Swagger / health / logging | `./mvnw clean verify` | 73 tests, zero failures/errors/skips |
| Final review fixes | `./mvnw clean verify` | 74 tests, zero failures/errors/skips |

## Regression evidence

- Before the hours fix, London 02:00 returned 201 instead of 422; the fixture's Alex Morgan update changed zero rows. Both assertions failed before the fix.
- Before outbox integration, booking could not produce a durable outbox row. Tests now prove atomic rollback on outbox insert failure, token fencing on stale claims, final-attempt expiry, stable event id/payload across retries and disjoint concurrent claims.
- Real `apache/kafka:4.3.2` tests verify scheduled booking → consumed CloudEvent → PUBLISHED, 3 partitions, appointment key, ce_type/ce_id headers and payload shape. A stopped broker does not fail booking and leaves an event for retry.
- A captured-log regression failed on Kafka's default payload logging and passes with the redacted producer listener.
- Real `redis:7.4-alpine` tests verify 1-hour catalog keys, cache hits, corrupt/incomplete/null JSON fallback. A refused Redis connection still allows a confirmed booking.
- Before Swagger/health/log additions, endpoints returned 404 and decision logs were absent. Tests now verify OpenAPI/Swagger, UP health, rejection reasons, confirmation only after commit, and no customer/VIN in decision/error logs.

- Fresh whole-branch review found two Important issues and no Critical/Minor findings. Both were reproduced before fixing: generated errors lacked the Problem Details schema; a broker unavailable at startup later produced a topic with 1 partition instead of 3.
- Expanded OpenAPI assertions now verify `application/problem+json`, `ApiProblem`, and `errorCode`. `KafkaStartupRecoveryIntegrationTest` pauses only its disposable fresh broker, starts the application, confirms a booking with a PENDING outbox event, observes a provisioning retry, resumes the broker, and verifies PUBLISHED plus 3 partitions and a consumed record. The relay reconciles the topic asynchronously before claiming events; consumer auto-creation is disabled.

## Final local demo

Observed on 2026-11-04 (Asia/Ho_Chi_Minh), using the rebuilt final application image and the exact requested Kafka/Redis/UI images. HTTP port 18080 was used because 8080 was already occupied. All published ports were bound to 127.0.0.1.

| Check | Observed result |
| --- | --- |
| Health / readiness | Both 200, UP |
| Swagger / OpenAPI | Swagger HTML 200; all documented booking errors use Problem Details with errorCode |
| POST / GET | 201 then 200 for appointment `51b17ac4-1091-4bbc-a82f-40e6512dd762` |
| Duplicate / outside hours | 409 VEHICLE_ALREADY_BOOKED; 422 OUTSIDE_OPERATING_HOURS |
| Transactional outbox | Event `de5cb20a-493a-4bdc-8909-665b32cb78cc`, PUBLISHED, attempts=1 |
| Kafka delivery | Demo consumer logged the same event/appointment on partition 1, offset 0; topic has 3 partitions |
| Redis catalog | Dealership and service-type keys present; TTLs 2561/2560 seconds remaining (earlier population observed 3599/3598 seconds) |
| Kafka UI | HTTP 200, keyloop-local ONLINE, KRaft, 1 broker |

The pre-existing PostgreSQL container/data and the process using port 8080 were preserved. Demo cleanup stops only the four newly started app/Kafka/UI/Redis services; volumes remain. The managed worktree is retained. No global Java/Maven or unrelated repository configuration was changed.

These checks do not imply production readiness: delivery remains at-least-once; downstream durable deduplication, idempotency/cancellation, catalog invalidation, manual replay, advanced metrics/tracing and production security/replication remain future work. See [implementation decisions](implementation-decisions.md).
