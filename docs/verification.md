# Verification record

This records observed results, separately from future-facing design claims. Commands ran inside the Java 21 Maven container described in README, with the Docker socket available to Testcontainers. No global Java/Maven settings were changed for this implementation.

| Completed stage | Command | Observed result |
| --- | --- | --- |
| Hours / concurrency fixture | `./mvnw clean verify` | 55 tests, zero failures/errors/skips |
| Transactional outbox / Kafka | `./mvnw clean verify` | 64 tests, zero failures/errors/skips |
| Redis catalog cache | `./mvnw clean verify` | 70 tests, zero failures/errors/skips |
| Swagger / health / logging | `./mvnw clean verify` | 73 tests, zero failures/errors/skips |

## Regression evidence

- Before the hours fix, London 02:00 returned 201 instead of 422; the fixture's Alex Morgan update changed zero rows. Both assertions failed before the fix.
- Before outbox integration, booking could not produce a durable outbox row. Tests now prove atomic rollback on outbox insert failure, token fencing on stale claims, final-attempt expiry, stable event id/payload across retries and disjoint concurrent claims.
- Real `apache/kafka:4.3.2` tests verify scheduled booking → consumed CloudEvent → PUBLISHED, 3 partitions, appointment key, ce_type/ce_id headers and payload shape. A stopped broker does not fail booking and leaves an event for retry.
- A captured-log regression failed on Kafka's default payload logging and passes with the redacted producer listener.
- Real `redis:7.4-alpine` tests verify 1-hour catalog keys, cache hits, corrupt/incomplete/null JSON fallback. A refused Redis connection still allows a confirmed booking.
- Before Swagger/health/log additions, endpoints returned 404 and decision logs were absent. Tests now verify OpenAPI/Swagger, UP health, rejection reasons, confirmation only after commit, and no customer/VIN in decision/error logs.

## Final local demo

Pending the final Compose smoke check. This section will be replaced with observed outcomes rather than assumed success.
