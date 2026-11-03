# Unified Service Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the Keyloop Scenario A backend with correct concurrent booking, automatic resource allocation, idempotency, availability search, Redis acceleration, Kafka outbox events, Kafka UI, Swagger UI, automated tests, and a reproducible demo.

**Architecture:** Java 21 and Spring Boot 4 expose REST APIs over explicit Spring JDBC repositories. PostgreSQL owns correctness through per-dealership advisory locks and GiST exclusion constraints; Redis is fail-open caching only; a transactional outbox publishes CloudEvents to Kafka without holding a database transaction across network I/O.

**Tech Stack:** Java 21, Spring Boot 4.1.2, Spring JDBC, PostgreSQL 16, Flyway, Redis 7.4, Apache Kafka 4 KRaft, kafbat Kafka UI, springdoc-openapi 3, JUnit 5, AssertJ, Testcontainers, Awaitility, Docker Compose.

**Specification:** `docs/system-design.md` v2.0 is the source of truth. Do not reintroduce Redisson locking or `FOR UPDATE SKIP LOCKED` into appointment allocation.

---

## File Map

```text
keyloop-service-scheduler/
├── compose.yaml
├── pom.xml
├── README.md
├── scripts/demo.sh
├── src/main/resources/
│   ├── application.yaml
│   └── db/migration/V1__schema.sql, V2__seed.sql
├── src/main/java/com/keyloop/scheduler/
│   ├── ServiceSchedulerApplication.java
│   ├── config/       ClockConfig, KafkaTopicConfig, SchedulerProperties
│   ├── support/      Sql, ResilientCache, TraceSupport
│   ├── catalog/      Dealership, ServiceType, CatalogRepository, CatalogService
│   ├── booking/      TimeSlot, OperatingHoursPolicy, SlotFinder, domain records,
│   │                 repositories, IdempotencyService, BookingService
│   ├── availability/ DayAvailability, AvailabilityService
│   ├── outbox/       AppointmentEventFactory, OutboxRepository, ClaimedEvent, OutboxRelay
│   ├── api/          controllers, request DTO, ApiExceptionHandler
│   └── demo/         AppointmentEventsLoggingConsumer
└── src/test/java/com/keyloop/scheduler/
    ├── support/      IntegrationTestBase
    ├── booking/      domain unit tests, service/concurrency/API tests
    ├── availability/ AvailabilityIntegrationTest
    └── outbox/       OutboxRelayIntegrationTest
```

### Task 0: Prepare the verified local toolchain

**Files:**
- No project files changed.

- [ ] **Step 1: Install and select JDK 21**

Ask for confirmation before installing software or changing shell environment, then run:

```bash
brew install openjdk@21
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Expected: `openjdk version "21..."`. Do not build with the machine's default JDK 8.

- [ ] **Step 2: Start Rancher Desktop and verify Docker**

```bash
~/.rd/bin/docker version
~/.rd/bin/docker info --format '{{.Architecture}}'
```

Expected: client and server sections are present; architecture is `aarch64` on the Apple Silicon host.

- [ ] **Step 3: Initialize the project repository**

```bash
cd /Users/ben.trungtran/Workspace/keyloop-service-scheduler
git init
git add docs/system-design.md docs/plans/2026-11-03-unified-service-scheduler.md
git commit -m "docs: define scheduler architecture and implementation plan"
```

Expected: one initial documentation commit. Confirm with the user before executing git operations.

### Task 1: Scaffold the Spring Boot application

**Files:**
- Create: `pom.xml`, Maven Wrapper files, `src/main/java/com/keyloop/scheduler/ServiceSchedulerApplication.java`
- Create: `src/main/resources/application.yaml`
- Create: `src/test/java/com/keyloop/scheduler/ServiceSchedulerApplicationTests.java`

- [ ] **Step 1: Generate the project with the verified Spring Initializr artifact names**

Run from the parent directory after confirming that the target contains only documentation:

```bash
cd /Users/ben.trungtran/Workspace/keyloop-service-scheduler
curl -fL 'https://start.spring.io/starter.zip?type=maven-project&language=java&bootVersion=4.1.2&baseDir=.&groupId=com.keyloop&artifactId=service-scheduler&name=service-scheduler&packageName=com.keyloop.scheduler&packaging=jar&javaVersion=21&dependencies=webmvc,validation,jdbc,data-redis,kafka,flyway,actuator,opentelemetry' -o /tmp/keyloop-scheduler.zip
unzip -o /tmp/keyloop-scheduler.zip -d /tmp/keyloop-scheduler-generated
rsync -a --exclude='docs' /tmp/keyloop-scheduler-generated/ ./
```

Expected: `./mvnw`, `pom.xml`, and the application package exist; `docs/` is unchanged.

- [ ] **Step 2: Add the remaining exact dependencies**

Add inside `<dependencies>` in `pom.xml`:

```xml
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-database-postgresql</artifactId>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>org.springdoc</groupId>
    <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
    <version>3.2.0</version>
</dependency>
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>org.awaitility</groupId>
    <artifactId>awaitility</artifactId>
    <version>4.3.0</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-testcontainers</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers-postgresql</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers-kafka</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers-junit-jupiter</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 3: Resolve dependencies and verify the wrapper**

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
./mvnw -q -DskipTests compile
```

Expected: exit code 0 and no dependency-resolution errors.

- [ ] **Step 4: Commit the scaffold**

```bash
git add pom.xml mvnw mvnw.cmd .mvn src .gitignore
git commit -m "build: scaffold Java 21 scheduler service"
```

### Task 2: Add local PostgreSQL, Redis, Kafka, and Kafka UI

**Files:**
- Create: `compose.yaml`
- Modify: `src/main/resources/application.yaml`

- [ ] **Step 1: Create the localhost-only infrastructure stack**

Create `compose.yaml`:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: scheduler
      POSTGRES_USER: scheduler
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "127.0.0.1:5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U scheduler -d scheduler"]
      interval: 2s
      timeout: 2s
      retries: 30
    volumes:
      - scheduler-postgres:/var/lib/postgresql/data

  redis:
    image: redis:7.4-alpine
    command: ["redis-server", "--appendonly", "yes"]
    ports:
      - "127.0.0.1:6380:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 2s
      retries: 30
    volumes:
      - scheduler-redis:/data

  kafka:
    image: apache/kafka:4.3.2
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:29093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
    ports:
      - "127.0.0.1:9092:9092"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 5s
      timeout: 5s
      retries: 30

  kafka-ui:
    image: kafbat/kafka-ui:v1.5.0
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    ports:
      - "127.0.0.1:8081:8080"

volumes:
  scheduler-postgres:
  scheduler-redis:
```

The database trust setting is deliberately limited to this disposable, localhost-only development container; never reuse it for deployed environments.

- [ ] **Step 2: Configure the application**

Create `src/main/resources/application.yaml`:

```yaml
spring:
  application:
    name: service-scheduler
  threads:
    virtual:
      enabled: true
  datasource:
    url: jdbc:postgresql://localhost:5432/scheduler
    username: scheduler
  data:
    redis:
      host: localhost
      port: 6380
  flyway:
    enabled: true
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        enable.idempotence: true
    consumer:
      group-id: notification-service-demo
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    template:
      observation-enabled: true
    listener:
      observation-enabled: true
  jackson:
    datatype:
      datetime:
        write-dates-as-timestamps: false

scheduler:
  topic: keyloop.ownership.appointments.v1
  outbox:
    enabled: true
    batch-size: 50
    publish-timeout: 5s
    max-attempts: 10
    fixed-delay: 500ms

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  opentelemetry:
    tracing:
      export:
        otlp:
          endpoint: http://localhost:4318/v1/traces
  otlp:
    metrics:
      export:
        enabled: false
  tracing:
    sampling:
      probability: 1.0

logging:
  structured:
    format:
      console: ecs

springdoc:
  swagger-ui:
    path: /swagger-ui.html
```

- [ ] **Step 3: Start and verify the containers**

```bash
~/.rd/bin/docker compose up -d postgres redis kafka kafka-ui
~/.rd/bin/docker compose ps
~/.rd/bin/docker exec keyloop-service-scheduler-redis-1 redis-cli ping
```

Expected: PostgreSQL, Redis, and Kafka are healthy; Redis prints `PONG`; Kafka UI is reachable at `http://localhost:8081` after startup.

- [ ] **Step 4: Commit local infrastructure**

```bash
git add compose.yaml src/main/resources/application.yaml
git commit -m "build: add local persistence cache and event infrastructure"
```

### Task 3: Prove database constraints and add schema/seed data

**Files:**
- Create: `src/test/java/com/keyloop/scheduler/support/IntegrationTestBase.java`
- Modify: `src/test/java/com/keyloop/scheduler/ServiceSchedulerApplicationTests.java`
- Create: `src/test/java/com/keyloop/scheduler/booking/SchemaConstraintsTest.java`
- Create: `src/main/resources/db/migration/V1__schema.sql`
- Create: `src/main/resources/db/migration/V2__seed.sql`

- [ ] **Step 1: Write the failing real-Postgres constraint tests**

Create `IntegrationTestBase.java`:

```java
package com.keyloop.scheduler.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
@SpringBootTest
public abstract class IntegrationTestBase {
    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection(name = "redis")
    static final GenericContainer<?> redis =
        new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2");
}
```

Replace the generated `ServiceSchedulerApplicationTests.java` so the context smoke test uses the same containers:

```java
package com.keyloop.scheduler;

import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;

class ServiceSchedulerApplicationTests extends IntegrationTestBase {
    @Test void contextLoads() {}
}
```

Create `SchemaConstraintsTest.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Transactional
class SchemaConstraintsTest extends IntegrationTestBase {
    private static final UUID DEALERSHIP = UUID.fromString("11111111-1111-1111-1111-111111111112");
    private static final UUID SERVICE_TYPE = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final UUID BAY = UUID.fromString("22222222-2222-2222-2222-222222222204");
    private static final UUID TECHNICIAN = UUID.fromString("33333333-3333-3333-3333-333333333304");

    @Autowired JdbcClient jdbc;

    @Test
    void rejectsOverlappingConfirmedAppointmentForSameBay() {
        insert("WBA3A5C51CF256789", "CONFIRMED", "2030-01-07T09:00:00Z", "2030-01-07T10:00:00Z");

        assertThatThrownBy(() ->
            insert("WBA3A5C51CF256780", "CONFIRMED", "2030-01-07T09:30:00Z", "2030-01-07T10:30:00Z"))
            .isInstanceOf(DataIntegrityViolationException.class)
            .hasMessageContaining("excl_appointments_")
            .hasMessageContaining("overlap");
    }

    @Test
    void allowsOverlapWhenExistingAppointmentIsCancelled() {
        insert("WBA3A5C51CF256789", "CANCELLED", "2030-01-07T09:00:00Z", "2030-01-07T10:00:00Z");
        insert("WBA3A5C51CF256780", "CONFIRMED", "2030-01-07T09:30:00Z", "2030-01-07T10:30:00Z");
    }

    private void insert(String vin, String status, String start, String end) {
        jdbc.sql("""
            INSERT INTO appointments (
              id, dealership_id, service_type_id, service_bay_id, technician_id,
              customer_id, vehicle_vin, start_time, end_time, status)
            VALUES (:id, :dealership, :serviceType, :bay, :technician,
              'CUST-TEST', :vin, :start, :end, :status)
            """)
            .param("id", UUID.randomUUID())
            .param("dealership", DEALERSHIP)
            .param("serviceType", SERVICE_TYPE)
            .param("bay", BAY)
            .param("technician", TECHNICIAN)
            .param("vin", vin)
            .param("start", OffsetDateTime.parse(start))
            .param("end", OffsetDateTime.parse(end))
            .param("status", status)
            .update();
    }
}
```

- [ ] **Step 2: Run the test and verify the schema is absent**

```bash
./mvnw -Dtest=SchemaConstraintsTest test
```

Expected: FAIL during context initialization because Flyway finds no schema migration.

- [ ] **Step 3: Add the complete schema**

Create `V1__schema.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE dealerships (
    id UUID PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    timezone VARCHAR(100) NOT NULL,
    open_time TIME NOT NULL,
    close_time TIME NOT NULL,
    CHECK (close_time > open_time)
);

CREATE TABLE service_types (
    id UUID PRIMARY KEY,
    code VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    duration_minutes INT NOT NULL CHECK (duration_minutes > 0),
    required_skill VARCHAR(100) NOT NULL
);

CREATE TABLE service_bays (
    id UUID PRIMARY KEY,
    dealership_id UUID NOT NULL REFERENCES dealerships(id),
    bay_code VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('ACTIVE', 'MAINTENANCE')),
    UNIQUE (dealership_id, bay_code)
);

CREATE TABLE technicians (
    id UUID PRIMARY KEY,
    dealership_id UUID NOT NULL REFERENCES dealerships(id),
    full_name VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('ACTIVE', 'INACTIVE'))
);

CREATE TABLE technician_skills (
    technician_id UUID NOT NULL REFERENCES technicians(id),
    skill_code VARCHAR(100) NOT NULL,
    PRIMARY KEY (technician_id, skill_code)
);

CREATE TABLE appointments (
    id UUID PRIMARY KEY,
    dealership_id UUID NOT NULL REFERENCES dealerships(id),
    service_type_id UUID NOT NULL REFERENCES service_types(id),
    service_bay_id UUID NOT NULL REFERENCES service_bays(id),
    technician_id UUID NOT NULL REFERENCES technicians(id),
    customer_id VARCHAR(100) NOT NULL,
    vehicle_vin VARCHAR(17) NOT NULL CHECK (vehicle_vin ~ '^[A-HJ-NPR-Z0-9]{17}$'),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    idempotency_key VARCHAR(255),
    request_hash VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at TIMESTAMPTZ,
    CHECK (end_time > start_time),
    CONSTRAINT uq_appointments_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT excl_appointments_bay_overlap EXCLUDE USING gist
      (service_bay_id WITH =, tstzrange(start_time, end_time, '[)') WITH &&)
      WHERE (status = 'CONFIRMED'),
    CONSTRAINT excl_appointments_tech_overlap EXCLUDE USING gist
      (technician_id WITH =, tstzrange(start_time, end_time, '[)') WITH &&)
      WHERE (status = 'CONFIRMED'),
    CONSTRAINT excl_appointments_vin_overlap EXCLUDE USING gist
      (vehicle_vin WITH =, tstzrange(start_time, end_time, '[)') WITH &&)
      WHERE (status = 'CONFIRMED')
);

CREATE INDEX idx_appointments_dealership_time
  ON appointments (dealership_id, start_time, end_time) WHERE status = 'CONFIRMED';

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    payload JSONB NOT NULL,
    trace_parent VARCHAR(255),
    status VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED', 'FAILED')),
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_claim ON outbox_events (created_at)
  WHERE status IN ('PENDING', 'IN_FLIGHT');
```

- [ ] **Step 4: Add deterministic demo data**

Create `V2__seed.sql`:

```sql
INSERT INTO dealerships (id, name, timezone, open_time, close_time) VALUES
('11111111-1111-1111-1111-111111111111', 'Keyloop Central Motors - London', 'Europe/London', '08:00', '18:00'),
('11111111-1111-1111-1111-111111111112', 'Keyloop North Motors - Manchester', 'Europe/London', '08:00', '18:00');

INSERT INTO service_types (id, code, name, duration_minutes, required_skill) VALUES
('44444444-4444-4444-4444-444444444401', 'OIL_CHANGE', 'Oil and Filter Change', 60, 'GENERAL_MAINTENANCE'),
('44444444-4444-4444-4444-444444444402', 'BRAKE_INSPECTION', 'Brake System Inspection', 45, 'BRAKE_SYSTEMS'),
('44444444-4444-4444-4444-444444444403', 'EV_BATTERY_DIAGNOSTICS', 'EV High-Voltage Battery Diagnostics', 120, 'EV_HIGH_VOLTAGE_CERT');

INSERT INTO service_bays (id, dealership_id, bay_code, status) VALUES
('22222222-2222-2222-2222-222222222201', '11111111-1111-1111-1111-111111111111', 'BAY-01', 'ACTIVE'),
('22222222-2222-2222-2222-222222222202', '11111111-1111-1111-1111-111111111111', 'BAY-02', 'ACTIVE'),
('22222222-2222-2222-2222-222222222203', '11111111-1111-1111-1111-111111111111', 'BAY-03', 'ACTIVE'),
('22222222-2222-2222-2222-222222222204', '11111111-1111-1111-1111-111111111112', 'BAY-01', 'ACTIVE');

INSERT INTO technicians (id, dealership_id, full_name, status) VALUES
('33333333-3333-3333-3333-333333333301', '11111111-1111-1111-1111-111111111111', 'Alex Mercer', 'ACTIVE'),
('33333333-3333-3333-3333-333333333302', '11111111-1111-1111-1111-111111111111', 'Priya Shah', 'ACTIVE'),
('33333333-3333-3333-3333-333333333303', '11111111-1111-1111-1111-111111111111', 'Tom Walker', 'ACTIVE'),
('33333333-3333-3333-3333-333333333304', '11111111-1111-1111-1111-111111111112', 'Minh Nguyen', 'ACTIVE');

INSERT INTO technician_skills (technician_id, skill_code) VALUES
('33333333-3333-3333-3333-333333333301', 'GENERAL_MAINTENANCE'),
('33333333-3333-3333-3333-333333333301', 'EV_HIGH_VOLTAGE_CERT'),
('33333333-3333-3333-3333-333333333302', 'GENERAL_MAINTENANCE'),
('33333333-3333-3333-3333-333333333303', 'BRAKE_SYSTEMS'),
('33333333-3333-3333-3333-333333333304', 'GENERAL_MAINTENANCE');
```

- [ ] **Step 5: Run the tests and commit**

```bash
./mvnw -Dtest=SchemaConstraintsTest test
```

Expected: PASS, including physical exclusion-constraint enforcement.

```bash
git add src/main/resources/db src/test/java/com/keyloop/scheduler
git commit -m "feat: enforce booking integrity in PostgreSQL"
```

### Task 4: Implement interval and operating-hours rules with pure unit tests

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/booking/TimeSlot.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/RejectionReason.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingRejectedException.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/OperatingHoursPolicy.java`
- Create: `src/main/java/com/keyloop/scheduler/catalog/Dealership.java`
- Create: `src/main/java/com/keyloop/scheduler/catalog/ServiceType.java`
- Create: `src/main/java/com/keyloop/scheduler/config/ClockConfig.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/TimeSlotTest.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/OperatingHoursPolicyTest.java`

- [ ] **Step 1: Write failing tests for all time boundaries**

Create `TimeSlotTest.java`:

```java
package com.keyloop.scheduler.booking;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.Instant;
import static org.assertj.core.api.Assertions.*;

class TimeSlotTest {
    private final TimeSlot nineToTen = TimeSlot.of(Instant.parse("2030-01-07T09:00:00Z"), Duration.ofHours(1));

    @Test void overlapIncludesContainment() {
        assertThat(nineToTen.overlaps(TimeSlot.of(Instant.parse("2030-01-07T09:15:00Z"), Duration.ofMinutes(10)))).isTrue();
        assertThat(nineToTen.overlaps(TimeSlot.of(Instant.parse("2030-01-07T09:30:00Z"), Duration.ofHours(1)))).isTrue();
    }

    @Test void backToBackDoesNotOverlap() {
        assertThat(nineToTen.overlaps(TimeSlot.of(Instant.parse("2030-01-07T10:00:00Z"), Duration.ofHours(1)))).isFalse();
        assertThat(nineToTen.overlaps(TimeSlot.of(Instant.parse("2030-01-07T08:00:00Z"), Duration.ofHours(1)))).isFalse();
    }

    @Test void rejectsNonPositiveInterval() {
        assertThatIllegalArgumentException().isThrownBy(() -> new TimeSlot(nineToTen.end(), nineToTen.start()));
    }
}
```

Create `OperatingHoursPolicyTest.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

class OperatingHoursPolicyTest {
    private final Clock clock = Clock.fixed(Instant.parse("2030-01-01T00:00:00Z"), ZoneOffset.UTC);
    private final OperatingHoursPolicy policy = new OperatingHoursPolicy(clock);
    private final Dealership london = new Dealership(
        UUID.fromString("11111111-1111-1111-1111-111111111111"),
        "London", "Europe/London", LocalTime.of(8, 0), LocalTime.of(18, 0));

    @Test void acceptsExactOpeningAndClosingBoundaries() {
        assertThatCode(() -> policy.validate(london,
            new TimeSlot(Instant.parse("2030-01-07T08:00:00Z"), Instant.parse("2030-01-07T18:00:00Z"))))
            .doesNotThrowAnyException();
    }

    @Test void rejectsAppointmentEndingAfterClose() {
        assertThatThrownBy(() -> policy.validate(london,
            new TimeSlot(Instant.parse("2030-01-07T17:30:00Z"), Instant.parse("2030-01-07T18:30:00Z"))))
            .extracting(e -> ((BookingRejectedException) e).reason())
            .isEqualTo(RejectionReason.OUTSIDE_OPERATING_HOURS);
    }

    @Test void rejectsPastStart() {
        assertThatThrownBy(() -> policy.validate(london,
            TimeSlot.of(Instant.parse("2029-12-31T23:00:00Z"), Duration.ofHours(1))))
            .extracting(e -> ((BookingRejectedException) e).reason())
            .isEqualTo(RejectionReason.START_TIME_IN_PAST);
    }

    @Test void evaluatesLondonSummerTimeInDealershipZone() {
        assertThatThrownBy(() -> policy.validate(london,
            TimeSlot.of(Instant.parse("2030-07-01T06:30:00Z"), Duration.ofHours(1))))
            .extracting(e -> ((BookingRejectedException) e).reason())
            .isEqualTo(RejectionReason.OUTSIDE_OPERATING_HOURS);
    }
}
```

- [ ] **Step 2: Run the tests and verify compilation fails**

```bash
./mvnw -Dtest=TimeSlotTest,OperatingHoursPolicyTest test
```

Expected: FAIL because the domain types do not exist.

- [ ] **Step 3: Implement the domain types exactly**

Create `TimeSlot.java`:

```java
package com.keyloop.scheduler.booking;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public record TimeSlot(Instant start, Instant end) {
    public TimeSlot {
        Objects.requireNonNull(start);
        Objects.requireNonNull(end);
        if (!end.isAfter(start)) throw new IllegalArgumentException("end must be after start");
    }
    public static TimeSlot of(Instant start, Duration duration) {
        return new TimeSlot(start, start.plus(duration));
    }
    public boolean overlaps(TimeSlot other) {
        return start.isBefore(other.end) && end.isAfter(other.start);
    }
}
```

Create `Dealership.java` and `ServiceType.java`:

```java
package com.keyloop.scheduler.catalog;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.UUID;

public record Dealership(UUID id, String name, String timezone, LocalTime openTime, LocalTime closeTime) {
    public ZoneId zoneId() { return ZoneId.of(timezone); }
}
```

```java
package com.keyloop.scheduler.catalog;

import java.time.Duration;
import java.util.UUID;

public record ServiceType(UUID id, String code, String name, int durationMinutes, String requiredSkill) {
    public Duration duration() { return Duration.ofMinutes(durationMinutes); }
}
```

Create `RejectionReason.java` and `BookingRejectedException.java`:

```java
package com.keyloop.scheduler.booking;

public enum RejectionReason {
    DEALERSHIP_NOT_FOUND, SERVICE_TYPE_NOT_FOUND, APPOINTMENT_NOT_FOUND,
    NO_SERVICE_BAY_AVAILABLE, NO_QUALIFIED_TECHNICIAN_AVAILABLE,
    VEHICLE_ALREADY_BOOKED, APPOINTMENT_ALREADY_CANCELLED,
    START_TIME_IN_PAST, OUTSIDE_OPERATING_HOURS, IDEMPOTENCY_KEY_REUSED
}
```

```java
package com.keyloop.scheduler.booking;

public class BookingRejectedException extends RuntimeException {
    private final RejectionReason reason;
    public BookingRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }
    public RejectionReason reason() { return reason; }
}
```

Create `OperatingHoursPolicy.java` and `ClockConfig.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;
import org.springframework.stereotype.Component;
import java.time.*;
import java.util.UUID;

@Component
public class OperatingHoursPolicy {
    private final Clock clock;
    public OperatingHoursPolicy(Clock clock) { this.clock = clock; }

    public void validate(Dealership dealership, TimeSlot slot) {
        if (!slot.start().isAfter(clock.instant())) {
            throw new BookingRejectedException(RejectionReason.START_TIME_IN_PAST,
                "desiredStartTime must be in the future");
        }
        ZoneId zone = dealership.zoneId();
        LocalDate date = slot.start().atZone(zone).toLocalDate();
        Instant open = date.atTime(dealership.openTime()).atZone(zone).toInstant();
        Instant close = date.atTime(dealership.closeTime()).atZone(zone).toInstant();
        if (slot.start().isBefore(open) || slot.end().isAfter(close)) {
            throw new BookingRejectedException(RejectionReason.OUTSIDE_OPERATING_HOURS,
                "Appointment must fit within opening hours " + dealership.openTime()
                    + "-" + dealership.closeTime() + " " + zone + " on " + date);
        }
    }
}
```

```java
package com.keyloop.scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Clock;

@Configuration
public class ClockConfig {
    @Bean Clock clock() { return Clock.systemUTC(); }
}
```

- [ ] **Step 4: Run tests and commit**

```bash
./mvnw -Dtest=TimeSlotTest,OperatingHoursPolicyTest test
```

Expected: all boundary, overlap, past-time, and BST tests PASS.

```bash
git add src/main/java/com/keyloop/scheduler src/test/java/com/keyloop/scheduler/booking
git commit -m "feat: add timezone-safe appointment domain rules"
```

### Task 5: Implement and test free-slot computation

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/booking/SlotFinder.java`
- Create: `src/main/java/com/keyloop/scheduler/availability/DayAvailability.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/SlotFinderTest.java`

- [ ] **Step 1: Write the failing slot-search tests**

Create `SlotFinderTest.java` with these complete fixtures and cases:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;
import com.keyloop.scheduler.catalog.ServiceType;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.assertThat;

class SlotFinderTest {
    private final SlotFinder finder = new SlotFinder();
    private final UUID bay = UUID.fromString("22222222-2222-2222-2222-222222222201");
    private final UUID technician = UUID.fromString("33333333-3333-3333-3333-333333333302");
    private final Dealership dealership = new Dealership(
        UUID.fromString("11111111-1111-1111-1111-111111111111"),
        "London", "Europe/London", LocalTime.of(8, 0), LocalTime.of(10, 0));
    private final ServiceType oil = new ServiceType(
        UUID.fromString("44444444-4444-4444-4444-444444444401"),
        "OIL_CHANGE", "Oil", 60, "GENERAL_MAINTENANCE");
    private final LocalDate date = LocalDate.of(2030, 1, 7);

    @Test void returnsThirtyMinuteGridWithinOpeningHours() {
        List<Instant> slots = find(Instant.parse("2030-01-01T00:00:00Z"), Map.of(), Map.of());
        assertThat(slots).containsExactly(
            Instant.parse("2030-01-07T08:00:00Z"), Instant.parse("2030-01-07T08:30:00Z"),
            Instant.parse("2030-01-07T09:00:00Z"));
    }

    @Test void removesSlotWhenOnlyBayOrTechnicianIsBusy() {
        TimeSlot busy = new TimeSlot(Instant.parse("2030-01-07T08:00:00Z"), Instant.parse("2030-01-07T09:00:00Z"));
        assertThat(find(Instant.parse("2030-01-01T00:00:00Z"), Map.of(bay, List.of(busy)), Map.of()))
            .doesNotContain(Instant.parse("2030-01-07T08:00:00Z"));
        assertThat(find(Instant.parse("2030-01-01T00:00:00Z"), Map.of(), Map.of(technician, List.of(busy))))
            .doesNotContain(Instant.parse("2030-01-07T08:00:00Z"));
    }

    @Test void keepsSlotWhenAnotherResourceIsFree() {
        UUID secondBay = UUID.fromString("22222222-2222-2222-2222-222222222202");
        TimeSlot busy = new TimeSlot(Instant.parse("2030-01-07T08:00:00Z"), Instant.parse("2030-01-07T09:00:00Z"));
        List<Instant> slots = finder.find(dealership, oil, date, Instant.parse("2030-01-01T00:00:00Z"),
            Set.of(bay, secondBay), Set.of(technician), Map.of(bay, List.of(busy)), Map.of());
        assertThat(slots).contains(Instant.parse("2030-01-07T08:00:00Z"));
    }

    @Test void excludesSlotsStartingBeforeNotBefore() {
        List<Instant> slots = find(Instant.parse("2030-01-07T08:45:00Z"), Map.of(), Map.of());
        assertThat(slots).containsExactly(Instant.parse("2030-01-07T09:00:00Z"));
    }

    private List<Instant> find(Instant notBefore, Map<UUID, List<TimeSlot>> bays,
                               Map<UUID, List<TimeSlot>> technicians) {
        return finder.find(dealership, oil, date, notBefore, Set.of(bay), Set.of(technician), bays, technicians);
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
./mvnw -Dtest=SlotFinderTest test
```

Expected: FAIL because `SlotFinder` is missing.

- [ ] **Step 3: Implement `SlotFinder` and the response record**

Create `SlotFinder.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;
import com.keyloop.scheduler.catalog.ServiceType;
import org.springframework.stereotype.Component;
import java.time.*;
import java.util.*;

@Component
public class SlotFinder {
    public List<Instant> find(Dealership dealership, ServiceType serviceType, LocalDate date,
            Instant notBefore, Set<UUID> bayIds, Set<UUID> technicianIds,
            Map<UUID, List<TimeSlot>> bayBookings, Map<UUID, List<TimeSlot>> technicianBookings) {
        List<Instant> result = new ArrayList<>();
        LocalDateTime cursor = date.atTime(dealership.openTime());
        LocalDateTime close = date.atTime(dealership.closeTime());
        while (!cursor.plus(serviceType.duration()).isAfter(close)) {
            Instant start = cursor.atZone(dealership.zoneId()).toInstant();
            TimeSlot candidate = TimeSlot.of(start, serviceType.duration());
            if (!start.isBefore(notBefore)
                    && hasFreeResource(bayIds, bayBookings, candidate)
                    && hasFreeResource(technicianIds, technicianBookings, candidate)) {
                result.add(start);
            }
            cursor = cursor.plusMinutes(30);
        }
        return List.copyOf(result);
    }

    private boolean hasFreeResource(Set<UUID> ids, Map<UUID, List<TimeSlot>> bookings, TimeSlot candidate) {
        return ids.stream().anyMatch(id -> bookings.getOrDefault(id, List.of()).stream()
            .noneMatch(candidate::overlaps));
    }
}
```

Create `DayAvailability.java`:

```java
package com.keyloop.scheduler.availability;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

public record DayAvailability(UUID dealershipId, UUID serviceTypeId, LocalDate date, List<Instant> availableStartTimes) {}
```

- [ ] **Step 4: Run tests and commit**

```bash
./mvnw -Dtest=TimeSlotTest,OperatingHoursPolicyTest,SlotFinderTest test
```

Expected: all domain tests PASS without starting Spring.

```bash
git add src/main/java/com/keyloop/scheduler/booking/SlotFinder.java src/main/java/com/keyloop/scheduler/availability/DayAvailability.java src/test/java/com/keyloop/scheduler/booking/SlotFinderTest.java
git commit -m "feat: compute available appointment start times"
```

### Task 6: Add fail-open Redis caching and catalog reads

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/support/ResilientCache.java`
- Create: `src/main/java/com/keyloop/scheduler/catalog/CatalogRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/catalog/CatalogService.java`
- Test: `src/test/java/com/keyloop/scheduler/support/ResilientCacheTest.java`

- [ ] **Step 1: Write the failing Redis-outage test**

Create `ResilientCacheTest.java`:

```java
package com.keyloop.scheduler.support;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

class ResilientCacheTest {
    @Test
    void redisFailureFallsBackWithoutThrowing() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked") ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.get("key")).thenThrow(new RedisConnectionFailureException("down"));
        doThrow(new RedisConnectionFailureException("down")).when(values)
            .set(anyString(), anyString(), any(java.time.Duration.class));
        SimpleMeterRegistry metrics = new SimpleMeterRegistry();
        ResilientCache cache = new ResilientCache(redis, JsonMapper.builder().build(), metrics);

        Optional<String> result = cache.get("key", String.class);

        assertThat(result).isEmpty();
        assertThatCode(() -> cache.put("key", "value", java.time.Duration.ofMinutes(1))).doesNotThrowAnyException();
        assertThat(metrics.get("scheduler.cache").tag("cache", "catalog").tag("result", "error").counter().count())
            .isEqualTo(2.0);
    }
}
```

- [ ] **Step 2: Verify failure and implement the cache**

```bash
./mvnw -Dtest=ResilientCacheTest test
```

Expected: FAIL because `ResilientCache` is missing.

Create `ResilientCache.java`:

```java
package com.keyloop.scheduler.support;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Optional;

@Component
public class ResilientCache {
    private final StringRedisTemplate redis;
    private final JsonMapper json;
    private final MeterRegistry metrics;

    public ResilientCache(StringRedisTemplate redis, JsonMapper json, MeterRegistry metrics) {
        this.redis = redis;
        this.json = json;
        this.metrics = metrics;
    }

    public <T> Optional<T> get(String key, Class<T> type) {
        return get("catalog", key, type);
    }

    public <T> Optional<T> get(String cache, String key, Class<T> type) {
        try {
            String value = redis.opsForValue().get(key);
            if (value == null) {
                count(cache, "miss");
                return Optional.empty();
            }
            count(cache, "hit");
            return Optional.of(json.readValue(value, type));
        } catch (Exception e) {
            count(cache, "error");
            return Optional.empty();
        }
    }

    public void put(String key, Object value, Duration ttl) {
        put("catalog", key, value, ttl);
    }

    public void put(String cache, String key, Object value, Duration ttl) {
        try {
            redis.opsForValue().set(key, json.writeValueAsString(value), ttl);
        } catch (Exception e) {
            count(cache, "error");
        }
    }

    public long increment(String key, Duration ttl) {
        try {
            Long value = redis.opsForValue().increment(key);
            if (value != null && value == 1L) redis.expire(key, ttl);
            return value == null ? 0L : value;
        } catch (Exception e) {
            count("availability", "error");
            return 0L;
        }
    }

    private void count(String cache, String result) {
        metrics.counter("scheduler.cache", "cache", cache, "result", result).increment();
    }
}
```

- [ ] **Step 3: Add catalog SQL and read-through caching**

Create `CatalogRepository.java`:

```java
package com.keyloop.scheduler.catalog;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import java.util.UUID;

@Repository
public class CatalogRepository {
    private final JdbcClient jdbc;
    public CatalogRepository(JdbcClient jdbc) { this.jdbc = jdbc; }

    public Optional<Dealership> findDealership(UUID id) {
        return jdbc.sql("SELECT id, name, timezone, open_time, close_time FROM dealerships WHERE id = :id")
            .param("id", id)
            .query((rs, n) -> new Dealership(
                rs.getObject("id", UUID.class), rs.getString("name"), rs.getString("timezone"),
                rs.getTime("open_time").toLocalTime(), rs.getTime("close_time").toLocalTime()))
            .optional();
    }

    public Optional<ServiceType> findServiceType(UUID id) {
        return jdbc.sql("SELECT id, code, name, duration_minutes, required_skill FROM service_types WHERE id = :id")
            .param("id", id)
            .query((rs, n) -> new ServiceType(
                rs.getObject("id", UUID.class), rs.getString("code"), rs.getString("name"),
                rs.getInt("duration_minutes"), rs.getString("required_skill")))
            .optional();
    }
}
```

Create `CatalogService.java`:

```java
package com.keyloop.scheduler.catalog;

import com.keyloop.scheduler.booking.BookingRejectedException;
import com.keyloop.scheduler.booking.RejectionReason;
import com.keyloop.scheduler.support.ResilientCache;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.UUID;

@Service
public class CatalogService {
    private static final Duration TTL = Duration.ofHours(1);
    private final CatalogRepository repository;
    private final ResilientCache cache;
    public CatalogService(CatalogRepository repository, ResilientCache cache) {
        this.repository = repository;
        this.cache = cache;
    }

    public Dealership dealership(UUID id) {
        String key = "scheduler:cache:dealership:" + id;
        return cache.get(key, Dealership.class)
            .or(() -> repository.findDealership(id).map(value -> {
                cache.put(key, value, TTL);
                return value;
            }))
            .orElseThrow(() -> new BookingRejectedException(
                RejectionReason.DEALERSHIP_NOT_FOUND, "Dealership not found: " + id));
    }

    public ServiceType serviceType(UUID id) {
        String key = "scheduler:cache:servicetype:" + id;
        return cache.get(key, ServiceType.class)
            .or(() -> repository.findServiceType(id).map(value -> {
                cache.put(key, value, TTL);
                return value;
            }))
            .orElseThrow(() -> new BookingRejectedException(
                RejectionReason.SERVICE_TYPE_NOT_FOUND, "Service type not found: " + id));
    }
}
```

- [ ] **Step 4: Run tests and commit**

```bash
./mvnw -Dtest=ResilientCacheTest test
```

Expected: PASS and two `scheduler.cache{result=error}` increments.

```bash
git add src/main/java/com/keyloop/scheduler/support/ResilientCache.java src/main/java/com/keyloop/scheduler/catalog src/test/java/com/keyloop/scheduler/support/ResilientCacheTest.java
git commit -m "feat: add fail-open catalog caching"
```

### Task 7: Add appointment models, allocation SQL, and outbox persistence

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/booking/AppointmentStatus.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/Appointment.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/ResourceRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/AppointmentRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/outbox/OutboxEvent.java`
- Create: `src/main/java/com/keyloop/scheduler/outbox/AppointmentEventFactory.java`
- Create: `src/main/java/com/keyloop/scheduler/outbox/OutboxRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/support/Sql.java`
- Create: `src/main/java/com/keyloop/scheduler/support/TraceSupport.java`

- [ ] **Step 1: Define the immutable appointment response model**

Create `AppointmentStatus.java` and `Appointment.java`:

```java
package com.keyloop.scheduler.booking;
public enum AppointmentStatus { CONFIRMED, CANCELLED }
```

```java
package com.keyloop.scheduler.booking;

import java.time.Instant;
import java.util.UUID;

public record Appointment(
    UUID id, UUID dealershipId, String dealershipName, String customerId, String vehicleVin,
    ServiceTypeRef serviceType, BayRef allocatedServiceBay, TechnicianRef assignedTechnician,
    Instant startTime, Instant endTime, AppointmentStatus status, Instant createdAt, Instant cancelledAt) {
    public record ServiceTypeRef(UUID id, String code, String name, int durationMinutes, String requiredSkill) {}
    public record BayRef(UUID id, String bayCode) {}
    public record TechnicianRef(UUID id, String fullName) {}
}
```

- [ ] **Step 2: Implement deterministic allocation with no bay/technician row skipping**

Create `ResourceRepository.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.ServiceType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ResourceRepository {
    public record BayResource(UUID id, String bayCode) {}
    public record TechnicianResource(UUID id, String fullName) {}
    private final JdbcClient jdbc;
    public ResourceRepository(JdbcClient jdbc) { this.jdbc = jdbc; }

    public void lockDealership(UUID dealershipId) {
        jdbc.sql("SELECT pg_advisory_xact_lock(hashtextextended(CAST(:dealershipId AS text), 0))")
            .param("dealershipId", dealershipId).query((rs, n) -> new Object()).single();
    }

    public Optional<BayResource> findFreeBay(UUID dealershipId, TimeSlot slot) {
        return jdbc.sql("""
            SELECT sb.id, sb.bay_code FROM service_bays sb
            WHERE sb.dealership_id = :dealershipId AND sb.status = 'ACTIVE'
              AND NOT EXISTS (
                SELECT 1 FROM appointments a
                WHERE a.service_bay_id = sb.id AND a.status = 'CONFIRMED'
                  AND a.start_time < :endTime AND a.end_time > :startTime)
            ORDER BY sb.bay_code ASC, sb.id ASC LIMIT 1
            """)
            .param("dealershipId", dealershipId)
            .param("startTime", OffsetDateTime.ofInstant(slot.start(), ZoneOffset.UTC))
            .param("endTime", OffsetDateTime.ofInstant(slot.end(), ZoneOffset.UTC))
            .query((rs, n) -> new BayResource(rs.getObject("id", UUID.class), rs.getString("bay_code")))
            .optional();
    }

    public Optional<TechnicianResource> findFreeTechnician(UUID dealershipId, ServiceType serviceType, TimeSlot slot) {
        return jdbc.sql("""
            SELECT t.id, t.full_name FROM technicians t
            WHERE t.dealership_id = :dealershipId AND t.status = 'ACTIVE'
              AND EXISTS (SELECT 1 FROM technician_skills ts
                WHERE ts.technician_id = t.id AND ts.skill_code = :skill)
              AND NOT EXISTS (
                SELECT 1 FROM appointments a
                WHERE a.technician_id = t.id AND a.status = 'CONFIRMED'
                  AND a.start_time < :endTime AND a.end_time > :startTime)
            ORDER BY (SELECT COUNT(*) FROM technician_skills ts2 WHERE ts2.technician_id = t.id) ASC,
              t.full_name ASC, t.id ASC LIMIT 1
            """)
            .param("dealershipId", dealershipId).param("skill", serviceType.requiredSkill())
            .param("startTime", OffsetDateTime.ofInstant(slot.start(), ZoneOffset.UTC))
            .param("endTime", OffsetDateTime.ofInstant(slot.end(), ZoneOffset.UTC))
            .query((rs, n) -> new TechnicianResource(rs.getObject("id", UUID.class), rs.getString("full_name")))
            .optional();
    }
}
```

- [ ] **Step 3: Implement appointment reads/writes and authoritative idempotency lookup**

Create `AppointmentRepository.java`:

```java
package com.keyloop.scheduler.booking;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

@Repository
public class AppointmentRepository {
    private static final String SELECT = """
        SELECT a.*, d.name dealership_name, st.code service_code, st.name service_name,
          st.duration_minutes, st.required_skill, sb.bay_code, t.full_name
        FROM appointments a
        JOIN dealerships d ON d.id = a.dealership_id
        JOIN service_types st ON st.id = a.service_type_id
        JOIN service_bays sb ON sb.id = a.service_bay_id
        JOIN technicians t ON t.id = a.technician_id
        """;
    private static final RowMapper<Appointment> MAPPER = (rs, n) -> new Appointment(
        rs.getObject("id", UUID.class), rs.getObject("dealership_id", UUID.class),
        rs.getString("dealership_name"), rs.getString("customer_id"), rs.getString("vehicle_vin"),
        new Appointment.ServiceTypeRef(rs.getObject("service_type_id", UUID.class), rs.getString("service_code"),
            rs.getString("service_name"), rs.getInt("duration_minutes"), rs.getString("required_skill")),
        new Appointment.BayRef(rs.getObject("service_bay_id", UUID.class), rs.getString("bay_code")),
        new Appointment.TechnicianRef(rs.getObject("technician_id", UUID.class), rs.getString("full_name")),
        rs.getObject("start_time", OffsetDateTime.class).toInstant(),
        rs.getObject("end_time", OffsetDateTime.class).toInstant(),
        AppointmentStatus.valueOf(rs.getString("status")),
        rs.getObject("created_at", OffsetDateTime.class).toInstant(),
        rs.getObject("cancelled_at", OffsetDateTime.class) == null ? null
            : rs.getObject("cancelled_at", OffsetDateTime.class).toInstant());

    private final JdbcClient jdbc;
    public AppointmentRepository(JdbcClient jdbc) { this.jdbc = jdbc; }

    public Optional<Appointment> findById(UUID id) {
        return jdbc.sql(SELECT + " WHERE a.id = :id").param("id", id).query(MAPPER).optional();
    }

    public Optional<Appointment> findByIdempotencyKey(String key) {
        return jdbc.sql(SELECT + " WHERE a.idempotency_key = :key").param("key", key).query(MAPPER).optional();
    }

    public boolean hasVehicleOverlap(String vin, TimeSlot slot) {
        return jdbc.sql("""
            SELECT EXISTS (SELECT 1 FROM appointments
              WHERE vehicle_vin = :vin AND status = 'CONFIRMED'
                AND start_time < :endTime AND end_time > :startTime)
            """)
            .param("vin", vin)
            .param("startTime", OffsetDateTime.ofInstant(slot.start(), ZoneOffset.UTC))
            .param("endTime", OffsetDateTime.ofInstant(slot.end(), ZoneOffset.UTC))
            .query(Boolean.class).single();
    }

    public void insert(Appointment a, String idempotencyKey, String requestHash) {
        jdbc.sql("""
            INSERT INTO appointments (id, dealership_id, service_type_id, service_bay_id, technician_id,
              customer_id, vehicle_vin, start_time, end_time, status, idempotency_key, request_hash, created_at)
            VALUES (:id, :dealershipId, :serviceTypeId, :bayId, :technicianId, :customerId, :vin,
              :startTime, :endTime, :status, :idempotencyKey, :requestHash, :createdAt)
            """)
            .param("id", a.id()).param("dealershipId", a.dealershipId())
            .param("serviceTypeId", a.serviceType().id()).param("bayId", a.allocatedServiceBay().id())
            .param("technicianId", a.assignedTechnician().id()).param("customerId", a.customerId())
            .param("vin", a.vehicleVin())
            .param("startTime", OffsetDateTime.ofInstant(a.startTime(), ZoneOffset.UTC))
            .param("endTime", OffsetDateTime.ofInstant(a.endTime(), ZoneOffset.UTC))
            .param("status", a.status().name()).param("idempotencyKey", idempotencyKey)
            .param("requestHash", requestHash)
            .param("createdAt", OffsetDateTime.ofInstant(a.createdAt(), ZoneOffset.UTC)).update();
    }

    public int cancel(UUID id) {
        return jdbc.sql("""
            UPDATE appointments SET status = 'CANCELLED', cancelled_at = now()
            WHERE id = :id AND status = 'CONFIRMED'
            """).param("id", id).update();
    }
}
```

- [ ] **Step 4: Implement CloudEvent creation and atomic outbox insertion**

Create `OutboxEvent.java`:

```java
package com.keyloop.scheduler.outbox;
import java.util.UUID;
public record OutboxEvent(UUID id, UUID appointmentId, String eventType, String payload, String traceParent) {}
```

Create `AppointmentEventFactory.java`:

```java
package com.keyloop.scheduler.outbox;

import com.keyloop.scheduler.booking.Appointment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class AppointmentEventFactory {
    private final JsonMapper json;
    public AppointmentEventFactory(JsonMapper json) { this.json = json; }

    public OutboxEvent confirmed(Appointment appointment, String traceParent) {
        return event(appointment, "confirmed", traceParent);
    }
    public OutboxEvent cancelled(Appointment appointment, String traceParent) {
        return event(appointment, "cancelled", traceParent);
    }
    private OutboxEvent event(Appointment a, String action, String traceParent) {
        UUID id = UUID.randomUUID();
        String type = "com.keyloop.ownership.appointment." + action + ".v1";
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("appointmentId", a.id());
        data.put("dealershipId", a.dealershipId());
        data.put("dealershipName", a.dealershipName());
        data.put("customerId", a.customerId());
        data.put("vehicleVin", a.vehicleVin());
        data.put("serviceType", a.serviceType());
        data.put("allocatedServiceBay", a.allocatedServiceBay());
        data.put("assignedTechnician", a.assignedTechnician());
        data.put("startTime", a.startTime());
        data.put("endTime", a.endTime());
        data.put("status", a.status());
        data.put("createdAt", a.createdAt());
        data.put("cancelledAt", a.cancelledAt());
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("specversion", "1.0");
        event.put("id", id);
        event.put("source", "/keyloop/ownership/unified-service-scheduler");
        event.put("type", type);
        event.put("subject", a.id());
        event.put("time", "cancelled".equals(action) ? a.cancelledAt() : a.createdAt());
        event.put("datacontenttype", "application/json");
        event.put("data", data);
        return new OutboxEvent(id, a.id(), type, json.writeValueAsString(event), traceParent);
    }
}
```

Create `OutboxRepository.java` with only the insert needed by booking; relay operations are added in Task 11:

```java
package com.keyloop.scheduler.outbox;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {
    private final JdbcClient jdbc;
    public OutboxRepository(JdbcClient jdbc) { this.jdbc = jdbc; }

    public void insert(OutboxEvent event) {
        jdbc.sql("""
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, trace_parent)
            VALUES (:id, 'Appointment', :appointmentId, :eventType, CAST(:payload AS jsonb), :traceParent)
            """)
            .param("id", event.id()).param("appointmentId", event.appointmentId())
            .param("eventType", event.eventType()).param("payload", event.payload())
            .param("traceParent", event.traceParent()).update();
    }
}
```

- [ ] **Step 5: Add SQL constraint extraction and trace propagation helpers**

Create `Sql.java`:

```java
package com.keyloop.scheduler.support;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Sql {
    private static final Pattern CONSTRAINT = Pattern.compile("violates (?:exclusion|unique) constraint \"([^\"]+)\"");
    private Sql() {}
    public static String constraintName(Throwable error) {
        Throwable current = error;
        while (current != null) {
            Matcher matcher = CONSTRAINT.matcher(current.getMessage() == null ? "" : current.getMessage());
            if (matcher.find()) return matcher.group(1);
            current = current.getCause();
        }
        return null;
    }
}
```

Create `TraceSupport.java`:

```java
package com.keyloop.scheduler.support;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import org.springframework.stereotype.Component;

@Component
public class TraceSupport {
    public String currentTraceParent() {
        SpanContext context = Span.current().getSpanContext();
        if (!context.isValid()) return null;
        return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex();
    }
}
```

- [ ] **Step 6: Compile and commit the persistence slice**

```bash
./mvnw -q -DskipTests compile
```

Expected: PASS. The next task supplies behavior-level repository tests through `BookingService`.

```bash
git add src/main/java/com/keyloop/scheduler/booking src/main/java/com/keyloop/scheduler/outbox src/main/java/com/keyloop/scheduler/support
git commit -m "feat: add deterministic allocation and outbox persistence"
```

### Task 8: Implement booking, idempotent replay, retrieval, and cancellation

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingCommand.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingResult.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/IdempotencyService.java`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingService.java`
- Modify: `src/main/java/com/keyloop/scheduler/booking/AppointmentRepository.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/BookingCommandTest.java`
- Test: `src/test/java/com/keyloop/scheduler/booking/BookingServiceIntegrationTest.java`

- [ ] **Step 1: Write the failing request-fingerprint test**

Create `BookingCommandTest.java`:

```java
package com.keyloop.scheduler.booking;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

class BookingCommandTest {
    private final BookingCommand command = new BookingCommand(
        UUID.fromString("11111111-1111-1111-1111-111111111111"), "CUST-1", "WBA3A5C51CF256789",
        UUID.fromString("44444444-4444-4444-4444-444444444401"), Instant.parse("2030-01-07T09:00:00Z"));

    @Test void fingerprintIsStableAndBodySensitive() {
        assertThat(command.requestHash()).isEqualTo(command.requestHash()).hasSize(64);
        BookingCommand changed = new BookingCommand(command.dealershipId(), "CUST-2", command.vehicleVin(),
            command.serviceTypeId(), command.desiredStartTime());
        assertThat(command.requestHash()).isNotEqualTo(changed.requestHash());
    }
}
```

- [ ] **Step 2: Write the failing service integration tests**

Create `BookingServiceIntegrationTest.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BookingServiceIntegrationTest extends IntegrationTestBase {
    private static final UUID LONDON = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID MANCHESTER = UUID.fromString("11111111-1111-1111-1111-111111111112");
    private static final UUID OIL = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final UUID EV = UUID.fromString("44444444-4444-4444-4444-444444444403");
    private static final Instant START = Instant.parse("2030-01-07T09:00:00Z");

    @Autowired BookingService service;
    @Autowired JdbcClient jdbc;

    @BeforeEach void clean() {
        jdbc.sql("DELETE FROM outbox_events").update();
        jdbc.sql("DELETE FROM appointments").update();
    }

    @Test void allocatesBayAndGeneralistWithFewestSkills() {
        BookingResult result = service.book(command(LONDON, OIL, "WBA3A5C51CF256789", START), null);

        assertThat(result.replayed()).isFalse();
        assertThat(result.appointment().allocatedServiceBay().bayCode()).isEqualTo("BAY-01");
        assertThat(result.appointment().assignedTechnician().fullName()).isEqualTo("Priya Shah");
        assertThat(result.appointment().endTime()).isEqualTo(START.plusSeconds(3600));
        assertThat(jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'")
            .query(Long.class).single()).isEqualTo(1L);
    }

    @Test void sameKeyAndBodyReplaysWithoutDuplicate() {
        BookingCommand command = command(LONDON, OIL, "WBA3A5C51CF256789", START);
        BookingResult first = service.book(command, "key-1");
        BookingResult replay = service.book(command, "key-1");

        assertThat(replay.replayed()).isTrue();
        assertThat(replay.appointment().id()).isEqualTo(first.appointment().id());
        assertThat(jdbc.sql("SELECT COUNT(*) FROM appointments").query(Long.class).single()).isEqualTo(1L);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM outbox_events").query(Long.class).single()).isEqualTo(1L);
    }

    @Test void sameKeyWithDifferentBodyIsRejected() {
        service.book(command(LONDON, OIL, "WBA3A5C51CF256789", START), "key-2");

        assertThatThrownBy(() -> service.book(
            command(LONDON, OIL, "WBA3A5C51CF256780", START), "key-2"))
            .isInstanceOfSatisfying(BookingRejectedException.class,
                e -> assertThat(e.reason()).isEqualTo(RejectionReason.IDEMPOTENCY_KEY_REUSED));
    }

    @Test void exhaustedSingleBayReturnsDomainConflict() {
        service.book(command(MANCHESTER, OIL, "WBA3A5C51CF256789", START), null);

        assertThatThrownBy(() -> service.book(command(MANCHESTER, OIL, "WBA3A5C51CF256780", START), null))
            .isInstanceOfSatisfying(BookingRejectedException.class,
                e -> assertThat(e.reason()).isEqualTo(RejectionReason.NO_SERVICE_BAY_AVAILABLE));
    }

    @Test void missingSkillReturnsTechnicianConflict() {
        assertThatThrownBy(() -> service.book(command(MANCHESTER, EV, "WBA3A5C51CF256789", START), null))
            .isInstanceOfSatisfying(BookingRejectedException.class,
                e -> assertThat(e.reason()).isEqualTo(RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE));
    }

    @Test void vehicleCannotOverlapEvenWhenOtherResourcesAreFree() {
        service.book(command(LONDON, OIL, "WBA3A5C51CF256789", START), null);

        assertThatThrownBy(() -> service.book(command(LONDON, OIL, "WBA3A5C51CF256789", START), null))
            .isInstanceOfSatisfying(BookingRejectedException.class,
                e -> assertThat(e.reason()).isEqualTo(RejectionReason.VEHICLE_ALREADY_BOOKED));
    }

    @Test void cancellationFreesResourcesAndEmitsSecondOutboxEvent() {
        Appointment first = service.book(command(MANCHESTER, OIL, "WBA3A5C51CF256789", START), null).appointment();
        Appointment cancelled = service.cancel(first.id());
        BookingResult second = service.book(command(MANCHESTER, OIL, "WBA3A5C51CF256780", START), null);

        assertThat(cancelled.status()).isEqualTo(AppointmentStatus.CANCELLED);
        assertThat(second.appointment().id()).isNotEqualTo(first.id());
        assertThat(jdbc.sql("SELECT COUNT(*) FROM outbox_events").query(Long.class).single()).isEqualTo(2L);
    }

    private BookingCommand command(UUID dealership, UUID serviceType, String vin, Instant start) {
        return new BookingCommand(dealership, "CUST-TEST", vin, serviceType, start);
    }
}
```

- [ ] **Step 3: Run the tests and verify failure**

```bash
./mvnw -Dtest=BookingCommandTest,BookingServiceIntegrationTest test
```

Expected: FAIL because booking orchestration is missing.

- [ ] **Step 4: Implement the command, replay cache, and transaction orchestration**

Create `BookingCommand.java`:

```java
package com.keyloop.scheduler.booking;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

public record BookingCommand(UUID dealershipId, String customerId, String vehicleVin,
                             UUID serviceTypeId, Instant desiredStartTime) {
    public String requestHash() {
        try {
            String canonical = dealershipId + "\n" + customerId + "\n" + vehicleVin + "\n"
                + serviceTypeId + "\n" + desiredStartTime;
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
```

Create `BookingResult.java`:

```java
package com.keyloop.scheduler.booking;
public record BookingResult(Appointment appointment, boolean replayed) {}
```

Create `IdempotencyService.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.support.ResilientCache;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

@Service
public class IdempotencyService {
    public record Replay(String requestHash, UUID appointmentId) {}
    private final ResilientCache cache;
    public IdempotencyService(ResilientCache cache) { this.cache = cache; }

    public Optional<Replay> find(String key) {
        if (key == null) return Optional.empty();
        return cache.get("idempotency", "scheduler:idempotency:" + key, Replay.class);
    }

    public void store(String key, String requestHash, UUID appointmentId) {
        if (key != null) cache.put("idempotency", "scheduler:idempotency:" + key,
            new Replay(requestHash, appointmentId), Duration.ofHours(24));
    }
}
```

Add this method to `AppointmentRepository.java` immediately after `findByIdempotencyKey`:

```java
public Optional<String> findRequestHash(String key) {
    return jdbc.sql("SELECT request_hash FROM appointments WHERE idempotency_key = :key")
        .param("key", key).query(String.class).optional();
}
```

Create `BookingService.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.CatalogService;
import com.keyloop.scheduler.catalog.Dealership;
import com.keyloop.scheduler.catalog.ServiceType;
import com.keyloop.scheduler.outbox.AppointmentEventFactory;
import com.keyloop.scheduler.outbox.OutboxRepository;
import com.keyloop.scheduler.support.ResilientCache;
import com.keyloop.scheduler.support.Sql;
import com.keyloop.scheduler.support.TraceSupport;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class BookingService {
    private final CatalogService catalog;
    private final OperatingHoursPolicy operatingHours;
    private final ResourceRepository resources;
    private final AppointmentRepository appointments;
    private final OutboxRepository outbox;
    private final AppointmentEventFactory events;
    private final IdempotencyService idempotency;
    private final ResilientCache cache;
    private final TraceSupport traces;
    private final TransactionTemplate transactions;
    private final Clock clock;

    public BookingService(CatalogService catalog, OperatingHoursPolicy operatingHours,
            ResourceRepository resources, AppointmentRepository appointments, OutboxRepository outbox,
            AppointmentEventFactory events, IdempotencyService idempotency, ResilientCache cache,
            TraceSupport traces, TransactionTemplate transactions, Clock clock) {
        this.catalog = catalog;
        this.operatingHours = operatingHours;
        this.resources = resources;
        this.appointments = appointments;
        this.outbox = outbox;
        this.events = events;
        this.idempotency = idempotency;
        this.cache = cache;
        this.traces = traces;
        this.transactions = transactions;
        this.clock = clock;
    }

    public BookingResult book(BookingCommand command, String key) {
        String requestHash = command.requestHash();
        if (key != null) {
            Optional<IdempotencyService.Replay> cached = idempotency.find(key);
            if (cached.isPresent()) {
                Optional<Appointment> existing = appointments.findById(cached.get().appointmentId());
                if (existing.isPresent()) return replay(existing.get(), key, requestHash);
            }
        }
        Dealership dealership = catalog.dealership(command.dealershipId());
        ServiceType serviceType = catalog.serviceType(command.serviceTypeId());
        TimeSlot slot = TimeSlot.of(command.desiredStartTime(), serviceType.duration());
        operatingHours.validate(dealership, slot);
        try {
            BookingResult result = Objects.requireNonNull(transactions.execute(tx ->
                bookInTransaction(command, key, requestHash, dealership, serviceType, slot)));
            if (!result.replayed()) {
                idempotency.store(key, requestHash, result.appointment().id());
                bumpAvailability(dealership, result.appointment().startTime().atZone(dealership.zoneId()).toLocalDate());
            }
            return result;
        } catch (DataIntegrityViolationException e) {
            return translateConstraint(e, key, requestHash);
        }
    }

    private BookingResult bookInTransaction(BookingCommand command, String key, String requestHash,
            Dealership dealership, ServiceType serviceType, TimeSlot slot) {
        resources.lockDealership(dealership.id());
        if (key != null) {
            Optional<Appointment> existing = appointments.findByIdempotencyKey(key);
            if (existing.isPresent()) return replay(existing.get(), key, requestHash);
        }
        if (appointments.hasVehicleOverlap(command.vehicleVin(), slot)) {
            throw new BookingRejectedException(RejectionReason.VEHICLE_ALREADY_BOOKED,
                "Vehicle already has a confirmed overlapping appointment");
        }
        ResourceRepository.BayResource bay = resources.findFreeBay(dealership.id(), slot)
            .orElseThrow(() -> new BookingRejectedException(RejectionReason.NO_SERVICE_BAY_AVAILABLE,
                "No active service bay is free for the requested interval"));
        ResourceRepository.TechnicianResource technician = resources
            .findFreeTechnician(dealership.id(), serviceType, slot)
            .orElseThrow(() -> new BookingRejectedException(RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE,
                "No active technician with skill " + serviceType.requiredSkill() + " is free"));
        Appointment appointment = new Appointment(UUID.randomUUID(), dealership.id(), dealership.name(),
            command.customerId(), command.vehicleVin(),
            new Appointment.ServiceTypeRef(serviceType.id(), serviceType.code(), serviceType.name(),
                serviceType.durationMinutes(), serviceType.requiredSkill()),
            new Appointment.BayRef(bay.id(), bay.bayCode()),
            new Appointment.TechnicianRef(technician.id(), technician.fullName()),
            slot.start(), slot.end(), AppointmentStatus.CONFIRMED, clock.instant(), null);
        appointments.insert(appointment, key, requestHash);
        outbox.insert(events.confirmed(appointment, traces.currentTraceParent()));
        return new BookingResult(appointment, false);
    }

    private BookingResult replay(Appointment appointment, String key, String requestHash) {
        String storedHash = appointments.findRequestHash(key).orElseThrow();
        if (!storedHash.equals(requestHash)) {
            throw new BookingRejectedException(RejectionReason.IDEMPOTENCY_KEY_REUSED,
                "Idempotency key was already used with a different request body");
        }
        return new BookingResult(appointment, true);
    }

    private BookingResult translateConstraint(DataIntegrityViolationException error, String key, String requestHash) {
        String constraint = Sql.constraintName(error);
        if ("uq_appointments_idempotency_key".equals(constraint) && key != null) {
            Appointment existing = appointments.findByIdempotencyKey(key).orElseThrow(() -> error);
            return replay(existing, key, requestHash);
        }
        RejectionReason reason = switch (constraint == null ? "" : constraint) {
            case "excl_appointments_vin_overlap" -> RejectionReason.VEHICLE_ALREADY_BOOKED;
            case "excl_appointments_bay_overlap" -> RejectionReason.NO_SERVICE_BAY_AVAILABLE;
            case "excl_appointments_tech_overlap" -> RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE;
            default -> throw error;
        };
        throw new BookingRejectedException(reason, "Concurrent booking conflict: " + reason);
    }

    public Appointment get(UUID id) {
        return appointments.findById(id).orElseThrow(() -> new BookingRejectedException(
            RejectionReason.APPOINTMENT_NOT_FOUND, "Appointment not found: " + id));
    }

    public Appointment cancel(UUID id) {
        Appointment result = Objects.requireNonNull(transactions.execute(tx -> {
            Appointment current = get(id);
            if (current.status() == AppointmentStatus.CANCELLED) {
                throw new BookingRejectedException(RejectionReason.APPOINTMENT_ALREADY_CANCELLED,
                    "Appointment is already cancelled: " + id);
            }
            if (appointments.cancel(id) != 1) {
                throw new BookingRejectedException(RejectionReason.APPOINTMENT_ALREADY_CANCELLED,
                    "Appointment was concurrently cancelled: " + id);
            }
            Appointment cancelled = get(id);
            outbox.insert(events.cancelled(cancelled, traces.currentTraceParent()));
            return cancelled;
        }));
        Dealership dealership = catalog.dealership(result.dealershipId());
        bumpAvailability(dealership, result.startTime().atZone(dealership.zoneId()).toLocalDate());
        return result;
    }

    private void bumpAvailability(Dealership dealership, LocalDate date) {
        cache.increment("scheduler:availability:version:" + dealership.id() + ":" + date, Duration.ofDays(2));
    }
}
```

- [ ] **Step 5: Run all booking tests and commit**

```bash
./mvnw -Dtest=BookingCommandTest,BookingServiceIntegrationTest test
```

Expected: all seven service tests and fingerprint tests PASS against real PostgreSQL/Redis/Kafka containers.

```bash
git add src/main/java/com/keyloop/scheduler/booking src/test/java/com/keyloop/scheduler/booking
git commit -m "feat: implement atomic idempotent appointment booking"
```

### Task 9: Prove concurrency correctness under the core acceptance scenarios

**Files:**
- Test: `src/test/java/com/keyloop/scheduler/booking/ConcurrentBookingTest.java`

- [ ] **Step 1: Write the failing/stress acceptance tests**

Create `ConcurrentBookingTest.java`:

```java
package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentBookingTest extends IntegrationTestBase {
    private static final UUID LONDON = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID MANCHESTER = UUID.fromString("11111111-1111-1111-1111-111111111112");
    private static final UUID OIL = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final Instant NINE = Instant.parse("2030-01-07T09:00:00Z");

    @Autowired BookingService service;
    @Autowired JdbcClient jdbc;

    @BeforeEach void clean() {
        jdbc.sql("DELETE FROM outbox_events").update();
        jdbc.sql("DELETE FROM appointments").update();
    }

    @Test void twentyThreadsOneBayProduceExactlyOneBooking() throws Exception {
        List<Object> outcomes = runConcurrently(20, i -> service.book(
            command(MANCHESTER, vin(i), NINE), null));

        assertThat(outcomes.stream().filter(BookingResult.class::isInstance)).hasSize(1);
        assertThat(outcomes.stream().filter(e -> hasReason(e, RejectionReason.NO_SERVICE_BAY_AVAILABLE))).hasSize(19);
        assertThat(countAppointments()).isEqualTo(1L);
    }

    @Test void adjacentAppointmentsDoNotCauseFalseConflict() throws Exception {
        List<Object> outcomes = runConcurrently(2, i -> service.book(
            command(MANCHESTER, vin(i), NINE.plusSeconds(i * 3600L)), null));

        assertThat(outcomes).allMatch(BookingResult.class::isInstance);
        assertThat(countAppointments()).isEqualTo(2L);
    }

    @Test void concurrentSameIdempotencyKeyCreatesOneAppointmentAndOneEvent() throws Exception {
        BookingCommand command = command(MANCHESTER, vin(1), NINE);
        List<Object> outcomes = runConcurrently(20, i -> service.book(command, "shared-key"));

        assertThat(outcomes).allMatch(BookingResult.class::isInstance);
        List<BookingResult> results = outcomes.stream().map(BookingResult.class::cast).toList();
        assertThat(results.stream().filter(r -> !r.replayed())).hasSize(1);
        assertThat(results).extracting(r -> r.appointment().id()).containsOnly(results.getFirst().appointment().id());
        assertThat(countAppointments()).isEqualTo(1L);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM outbox_events").query(Long.class).single()).isEqualTo(1L);
    }

    @Test void vinConstraintResolvesCrossDealershipRace() throws Exception {
        List<Object> outcomes = runConcurrently(2, i -> service.book(
            command(i == 0 ? LONDON : MANCHESTER, vin(1), NINE), null));

        assertThat(outcomes.stream().filter(BookingResult.class::isInstance)).hasSize(1);
        assertThat(outcomes.stream().filter(e -> hasReason(e, RejectionReason.VEHICLE_ALREADY_BOOKED))).hasSize(1);
        assertThat(countAppointments()).isEqualTo(1L);
    }

    private List<Object> runConcurrently(int count, BookingCall call) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(count);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Object>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int index = i;
                futures.add(executor.submit(() -> {
                    start.await();
                    try {
                        return call.book(index);
                    } catch (BookingRejectedException e) {
                        return e;
                    }
                }));
            }
            start.countDown();
            List<Object> outcomes = new ArrayList<>();
            for (Future<Object> future : futures) outcomes.add(future.get(20, TimeUnit.SECONDS));
            return outcomes;
        } finally {
            executor.shutdownNow();
        }
    }

    private BookingCommand command(UUID dealership, String vin, Instant start) {
        return new BookingCommand(dealership, "CUST-TEST", vin, OIL, start);
    }
    private String vin(int i) { return String.format("WBA3A5C51CF25%04d", i); }
    private long countAppointments() { return jdbc.sql("SELECT COUNT(*) FROM appointments").query(Long.class).single(); }
    private boolean hasReason(Object value, RejectionReason reason) {
        return value instanceof BookingRejectedException e && e.reason() == reason;
    }
    @FunctionalInterface interface BookingCall { Object book(int index); }
}
```

- [ ] **Step 2: Run the stress tests repeatedly**

```bash
./mvnw -Dtest=ConcurrentBookingTest test
./mvnw -Dtest=ConcurrentBookingTest test
```

Expected: both runs PASS. Any 500-equivalent exception, more than one overlapping row, false conflict on adjacent appointments, or duplicate idempotent insert blocks completion.

- [ ] **Step 3: Commit the correctness gate**

```bash
git add src/test/java/com/keyloop/scheduler/booking/ConcurrentBookingTest.java
git commit -m "test: prove concurrent booking correctness"
```

### Task 10: Implement versioned, fail-open availability caching

**Files:**
- Modify: `src/main/java/com/keyloop/scheduler/support/ResilientCache.java`
- Create: `src/main/java/com/keyloop/scheduler/availability/AvailabilityRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/availability/AvailabilityService.java`
- Test: `src/test/java/com/keyloop/scheduler/availability/AvailabilityIntegrationTest.java`

- [ ] **Step 1: Write the failing availability integration test**

Create `AvailabilityIntegrationTest.java`:

```java
package com.keyloop.scheduler.availability;

import com.keyloop.scheduler.booking.*;
import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AvailabilityIntegrationTest extends IntegrationTestBase {
    private static final UUID MANCHESTER = UUID.fromString("11111111-1111-1111-1111-111111111112");
    private static final UUID OIL = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final LocalDate DATE = LocalDate.of(2030, 1, 7);
    private static final Instant NINE = Instant.parse("2030-01-07T09:00:00Z");

    @Autowired AvailabilityService availability;
    @Autowired BookingService bookings;
    @Autowired JdbcClient jdbc;

    @BeforeEach void clean() {
        jdbc.sql("DELETE FROM outbox_events").update();
        jdbc.sql("DELETE FROM appointments").update();
    }

    @Test void bookingAndCancellationInvalidateCachedDay() {
        assertThat(availability.find(MANCHESTER, OIL, DATE).availableStartTimes()).contains(NINE);
        assertThat(availability.find(MANCHESTER, OIL, DATE).availableStartTimes()).contains(NINE);

        Appointment appointment = bookings.book(new BookingCommand(
            MANCHESTER, "CUST-TEST", "WBA3A5C51CF256789", OIL, NINE), null).appointment();
        assertThat(availability.find(MANCHESTER, OIL, DATE).availableStartTimes()).doesNotContain(NINE);

        bookings.cancel(appointment.id());
        assertThat(availability.find(MANCHESTER, OIL, DATE).availableStartTimes()).contains(NINE);
    }
}
```

- [ ] **Step 2: Verify failure and add raw Redis version reads**

```bash
./mvnw -Dtest=AvailabilityIntegrationTest test
```

Expected: FAIL because `AvailabilityService` is missing.

Add to `ResilientCache.java`:

```java
public long version(String key) {
    try {
        String value = redis.opsForValue().get(key);
        return value == null ? 0L : Long.parseLong(value);
    } catch (Exception e) {
        count("availability", "error");
        return 0L;
    }
}
```

- [ ] **Step 3: Implement availability queries and versioned caching**

Create `AvailabilityRepository.java`:

```java
package com.keyloop.scheduler.availability;

import com.keyloop.scheduler.booking.TimeSlot;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class AvailabilityRepository {
    private final JdbcClient jdbc;
    public AvailabilityRepository(JdbcClient jdbc) { this.jdbc = jdbc; }

    public Set<UUID> activeBays(UUID dealershipId) {
        return jdbc.sql("SELECT id FROM service_bays WHERE dealership_id = :id AND status = 'ACTIVE'")
            .param("id", dealershipId).query(UUID.class).set();
    }

    public Set<UUID> qualifiedTechnicians(UUID dealershipId, String skill) {
        return jdbc.sql("""
            SELECT t.id FROM technicians t JOIN technician_skills ts ON ts.technician_id = t.id
            WHERE t.dealership_id = :id AND t.status = 'ACTIVE' AND ts.skill_code = :skill
            """).param("id", dealershipId).param("skill", skill).query(UUID.class).set();
    }

    public Map<UUID, List<TimeSlot>> bayBookings(UUID dealershipId, TimeSlot day) {
        return bookings(dealershipId, day, "service_bay_id");
    }

    public Map<UUID, List<TimeSlot>> technicianBookings(UUID dealershipId, TimeSlot day) {
        return bookings(dealershipId, day, "technician_id");
    }

    private Map<UUID, List<TimeSlot>> bookings(UUID dealershipId, TimeSlot day, String resourceColumn) {
        if (!Set.of("service_bay_id", "technician_id").contains(resourceColumn)) {
            throw new IllegalArgumentException("Unsupported resource column");
        }
        record Busy(UUID resourceId, TimeSlot slot) {}
        List<Busy> rows = jdbc.sql("SELECT " + resourceColumn + " resource_id, start_time, end_time FROM appointments "
                + "WHERE dealership_id = :id AND status = 'CONFIRMED' "
                + "AND start_time < :dayEnd AND end_time > :dayStart")
            .param("id", dealershipId)
            .param("dayStart", OffsetDateTime.ofInstant(day.start(), ZoneOffset.UTC))
            .param("dayEnd", OffsetDateTime.ofInstant(day.end(), ZoneOffset.UTC))
            .query((rs, n) -> new Busy(rs.getObject("resource_id", UUID.class),
                new TimeSlot(rs.getObject("start_time", OffsetDateTime.class).toInstant(),
                    rs.getObject("end_time", OffsetDateTime.class).toInstant())))
            .list();
        return rows.stream().collect(Collectors.groupingBy(Busy::resourceId,
            Collectors.mapping(Busy::slot, Collectors.toList())));
    }
}
```

Create `AvailabilityService.java`:

```java
package com.keyloop.scheduler.availability;

import com.keyloop.scheduler.booking.SlotFinder;
import com.keyloop.scheduler.booking.TimeSlot;
import com.keyloop.scheduler.catalog.CatalogService;
import com.keyloop.scheduler.catalog.Dealership;
import com.keyloop.scheduler.catalog.ServiceType;
import com.keyloop.scheduler.support.ResilientCache;
import org.springframework.stereotype.Service;

import java.time.*;
import java.util.Set;
import java.util.UUID;

@Service
public class AvailabilityService {
    private final CatalogService catalog;
    private final AvailabilityRepository repository;
    private final SlotFinder finder;
    private final ResilientCache cache;
    private final Clock clock;

    public AvailabilityService(CatalogService catalog, AvailabilityRepository repository,
            SlotFinder finder, ResilientCache cache, Clock clock) {
        this.catalog = catalog;
        this.repository = repository;
        this.finder = finder;
        this.cache = cache;
        this.clock = clock;
    }

    public DayAvailability find(UUID dealershipId, UUID serviceTypeId, LocalDate date) {
        Dealership dealership = catalog.dealership(dealershipId);
        ServiceType serviceType = catalog.serviceType(serviceTypeId);
        String versionKey = "scheduler:availability:version:" + dealershipId + ":" + date;
        long version = cache.version(versionKey);
        String key = "scheduler:cache:availability:" + dealershipId + ":" + date + ":"
            + serviceTypeId + ":v" + version;
        return cache.get("availability", key, DayAvailability.class).orElseGet(() -> {
            Instant dayStart = date.atTime(dealership.openTime()).atZone(dealership.zoneId()).toInstant();
            Instant dayEnd = date.atTime(dealership.closeTime()).atZone(dealership.zoneId()).toInstant();
            TimeSlot day = new TimeSlot(dayStart, dayEnd);
            DayAvailability result = new DayAvailability(dealershipId, serviceTypeId, date,
                finder.find(dealership, serviceType, date, clock.instant(),
                    repository.activeBays(dealershipId),
                    repository.qualifiedTechnicians(dealershipId, serviceType.requiredSkill()),
                    repository.bayBookings(dealershipId, day),
                    repository.technicianBookings(dealershipId, day)));
            cache.put("availability", key, result, Duration.ofSeconds(60));
            return result;
        });
    }
}
```

- [ ] **Step 4: Run tests and commit**

```bash
./mvnw -Dtest=AvailabilityIntegrationTest,SlotFinderTest test
```

Expected: both PASS; cached reads return the same slots, booking bumps the version, and cancellation restores the slot.

```bash
git add src/main/java/com/keyloop/scheduler/availability src/main/java/com/keyloop/scheduler/support/ResilientCache.java src/test/java/com/keyloop/scheduler/availability
git commit -m "feat: cache advisory availability by dealership day"
```

### Task 11: Publish outbox events to Kafka and verify with a real consumer

**Files:**
- Modify: `src/main/java/com/keyloop/scheduler/ServiceSchedulerApplication.java`
- Create: `src/main/java/com/keyloop/scheduler/config/SchedulerProperties.java`
- Create: `src/main/java/com/keyloop/scheduler/config/KafkaTopicConfig.java`
- Create: `src/main/java/com/keyloop/scheduler/outbox/ClaimedEvent.java`
- Modify: `src/main/java/com/keyloop/scheduler/outbox/OutboxRepository.java`
- Create: `src/main/java/com/keyloop/scheduler/outbox/OutboxRelay.java`
- Create: `src/main/java/com/keyloop/scheduler/demo/AppointmentEventsLoggingConsumer.java`
- Modify: `src/test/java/com/keyloop/scheduler/support/IntegrationTestBase.java`
- Test: `src/test/java/com/keyloop/scheduler/outbox/OutboxRelayIntegrationTest.java`

- [ ] **Step 1: Write the failing end-to-end Kafka test**

Change the base annotation to disable the background relay during deterministic tests:

```java
@SpringBootTest(properties = "scheduler.outbox.enabled=false")
public abstract class IntegrationTestBase {
```

Create `OutboxRelayIntegrationTest.java`:

```java
package com.keyloop.scheduler.outbox;

import com.keyloop.scheduler.booking.*;
import com.keyloop.scheduler.support.IntegrationTestBase;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxRelayIntegrationTest extends IntegrationTestBase {
    @Autowired BookingService bookings;
    @Autowired OutboxRelay relay;
    @Autowired OutboxRepository outbox;
    @Autowired JdbcClient jdbc;
    @Value("${scheduler.topic}") String topic;
    @Value("${spring.kafka.bootstrap-servers}") String bootstrap;

    @BeforeEach void clean() {
        jdbc.sql("DELETE FROM outbox_events").update();
        jdbc.sql("DELETE FROM appointments").update();
    }

    @Test void bookingPublishesOrderedCloudEventAndMarksOutboxPublished() {
        Appointment appointment = bookings.book(new BookingCommand(
            UUID.fromString("11111111-1111-1111-1111-111111111111"), "CUST-KAFKA",
            "WBA3A5C51CF256789", UUID.fromString("44444444-4444-4444-4444-444444444401"),
            Instant.parse("2030-01-07T09:00:00Z")), null).appointment();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            AtomicReference<ConsumerRecord<String, String>> received = new AtomicReference<>();
            Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                relay.poll();
                consumer.poll(Duration.ofMillis(200)).forEach(record -> {
                    if (record.key().equals(appointment.id().toString())) received.set(record);
                });
                return received.get() != null;
            });

            ConsumerRecord<String, String> record = received.get();
            assertThat(record.key()).isEqualTo(appointment.id().toString());
            assertThat(new String(record.headers().lastHeader("ce_type").value(), StandardCharsets.UTF_8))
                .isEqualTo("com.keyloop.ownership.appointment.confirmed.v1");
            assertThat(record.value()).contains(appointment.id().toString(), "CONFIRMED", "WBA3A5C51CF256789");
            assertThat(outbox.statusFor(appointment.id())).isEqualTo("PUBLISHED");
        }
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
./mvnw -Dtest=OutboxRelayIntegrationTest test
```

Expected: FAIL because `OutboxRelay` and claim operations are missing.

- [ ] **Step 3: Enable scheduling/configuration and create the topic**

Update the application class:

```java
package com.keyloop.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@ConfigurationPropertiesScan
@SpringBootApplication
public class ServiceSchedulerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ServiceSchedulerApplication.class, args);
    }
}
```

Create `SchedulerProperties.java`:

```java
package com.keyloop.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@ConfigurationProperties("scheduler")
public record SchedulerProperties(String topic, Outbox outbox) {
    public record Outbox(int batchSize, Duration publishTimeout, int maxAttempts,
                         boolean enabled, Duration fixedDelay) {}
}
```

Create `KafkaTopicConfig.java`:

```java
package com.keyloop.scheduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {
    @Bean NewTopic appointmentTopic(SchedulerProperties properties) {
        return new NewTopic(properties.topic(), 3, (short) 1);
    }
}
```

Create `ClaimedEvent.java`:

```java
package com.keyloop.scheduler.outbox;
import java.util.UUID;
public record ClaimedEvent(UUID id, UUID appointmentId, String eventType,
                           String payload, String traceParent, int attempts) {}
```

- [ ] **Step 4: Add short-transaction claim and result updates**

Add these methods to `OutboxRepository.java` plus `import java.util.*;`, `import java.util.UUID;`, and `import org.springframework.transaction.annotation.Transactional;`:

```java
@Transactional
public List<ClaimedEvent> claim(int batchSize) {
    return jdbc.sql("""
        UPDATE outbox_events
        SET status = 'IN_FLIGHT', claimed_at = now(), attempts = attempts + 1
        WHERE id IN (
          SELECT id FROM outbox_events
          WHERE status = 'PENDING'
             OR (status = 'IN_FLIGHT' AND claimed_at < now() - interval '30 seconds')
          ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT :batchSize)
        RETURNING id, aggregate_id, event_type, payload, trace_parent, attempts
        """)
        .param("batchSize", batchSize)
        .query((rs, n) -> new ClaimedEvent(rs.getObject("id", UUID.class),
            rs.getObject("aggregate_id", UUID.class), rs.getString("event_type"),
            rs.getString("payload"), rs.getString("trace_parent"), rs.getInt("attempts")))
        .list();
}

public void markPublished(UUID id) {
    jdbc.sql("UPDATE outbox_events SET status = 'PUBLISHED', published_at = now(), last_error = NULL WHERE id = :id")
        .param("id", id).update();
}

public void markFailure(UUID id, String error, int maxAttempts) {
    jdbc.sql("""
        UPDATE outbox_events SET status = CASE WHEN attempts >= :maxAttempts THEN 'FAILED' ELSE 'PENDING' END,
          last_error = :error, claimed_at = NULL WHERE id = :id
        """)
        .param("id", id).param("error", error).param("maxAttempts", maxAttempts).update();
}

public long countPending() {
    return jdbc.sql("SELECT COUNT(*) FROM outbox_events WHERE status IN ('PENDING', 'IN_FLIGHT')")
        .query(Long.class).single();
}

public String statusFor(UUID appointmentId) {
    return jdbc.sql("SELECT status FROM outbox_events WHERE aggregate_id = :id")
        .param("id", appointmentId).query(String.class).single();
}
```

- [ ] **Step 5: Implement publishing outside database transactions**

Create `OutboxRelay.java`:

```java
package com.keyloop.scheduler.outbox;

import com.keyloop.scheduler.config.SchedulerProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final SchedulerProperties properties;
    private final MeterRegistry metrics;

    public OutboxRelay(OutboxRepository repository, KafkaTemplate<String, String> kafka,
            SchedulerProperties properties, MeterRegistry metrics) {
        this.repository = repository;
        this.kafka = kafka;
        this.properties = properties;
        this.metrics = metrics;
        Gauge.builder("scheduler.outbox.pending", repository, OutboxRepository::countPending).register(metrics);
    }

    @Scheduled(fixedDelayString = "${scheduler.outbox.fixed-delay:500ms}")
    public void scheduledPoll() {
        if (properties.outbox().enabled()) poll();
    }

    public void poll() {
        for (ClaimedEvent event : repository.claim(properties.outbox().batchSize())) publish(event);
    }

    private void publish(ClaimedEvent event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                properties.topic(), event.appointmentId().toString(), event.payload());
            header(record, "ce_type", event.eventType());
            header(record, "ce_id", event.id().toString());
            header(record, "traceparent", event.traceParent());
            kafka.send(record).get(properties.outbox().publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
            repository.markPublished(event.id());
            metrics.counter("scheduler.outbox.published").increment();
        } catch (Exception e) {
            repository.markFailure(event.id(), e.getMessage(), properties.outbox().maxAttempts());
            metrics.counter("scheduler.outbox.failed").increment();
            log.warn("Outbox publish failed eventId={} attempts={}", event.id(), event.attempts(), e);
        }
    }

    private void header(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
```

Create `AppointmentEventsLoggingConsumer.java`:

```java
package com.keyloop.scheduler.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AppointmentEventsLoggingConsumer {
    private static final Logger log = LoggerFactory.getLogger(AppointmentEventsLoggingConsumer.class);

    @KafkaListener(topics = "${scheduler.topic}", groupId = "notification-service-demo")
    public void onEvent(ConsumerRecord<String, String> record) {
        log.info("Appointment event received key={} partition={} offset={} body={}",
            record.key(), record.partition(), record.offset(), record.value());
    }
}
```

- [ ] **Step 6: Run the real-Kafka test and commit**

Run:

```bash
./mvnw -Dtest=OutboxRelayIntegrationTest test
```

Expected: PASS with real Kafka; key is `appointmentId`, CloudEvent headers/body are correct, and the row becomes `PUBLISHED`.

```bash
git add src/main/java/com/keyloop/scheduler src/test/java/com/keyloop/scheduler/support/IntegrationTestBase.java src/test/java/com/keyloop/scheduler/outbox
git commit -m "feat: publish transactional outbox events to Kafka"
```

### Task 12: Expose the REST/OpenAPI contract and Problem Details errors

**Files:**
- Create: `src/main/java/com/keyloop/scheduler/api/CreateAppointmentRequest.java`
- Create: `src/main/java/com/keyloop/scheduler/api/AppointmentController.java`
- Create: `src/main/java/com/keyloop/scheduler/api/AvailabilityController.java`
- Create: `src/main/java/com/keyloop/scheduler/api/ApiExceptionHandler.java`
- Test: `src/test/java/com/keyloop/scheduler/api/AppointmentApiTest.java`

- [ ] **Step 1: Write failing MockMvc contract tests**

Create `AppointmentApiTest.java`:

```java
package com.keyloop.scheduler.api;

import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@AutoConfigureMockMvc
class AppointmentApiTest extends IntegrationTestBase {
    @Autowired MockMvc mvc;
    @Autowired JdbcClient jdbc;

    @BeforeEach void clean() {
        jdbc.sql("DELETE FROM outbox_events").update();
        jdbc.sql("DELETE FROM appointments").update();
    }

    @Test void createsReadsAndCancelsAppointment() throws Exception {
        String response = mvc.perform(post("/api/v1/appointments")
                .header("Idempotency-Key", "api-key-1").contentType("application/json").content(body("WBA3A5C51CF256789")))
            .andExpect(status().isCreated())
            .andExpect(header().string("Location", containsString("/api/v1/appointments/")))
            .andExpect(header().string("Idempotent-Replayed", "false"))
            .andExpect(jsonPath("$.allocatedServiceBay.bayCode").value("BAY-01"))
            .andReturn().getResponse().getContentAsString();
        String id = com.jayway.jsonpath.JsonPath.read(response, "$.id");

        mvc.perform(get("/api/v1/appointments/{id}", id)).andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("CONFIRMED"));
        mvc.perform(post("/api/v1/appointments/{id}/cancel", id)).andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test void replaysSameIdempotentRequest() throws Exception {
        mvc.perform(post("/api/v1/appointments").header("Idempotency-Key", "api-key-2")
            .contentType("application/json").content(body("WBA3A5C51CF256789"))).andExpect(status().isCreated());
        mvc.perform(post("/api/v1/appointments").header("Idempotency-Key", "api-key-2")
                .contentType("application/json").content(body("WBA3A5C51CF256789")))
            .andExpect(status().isCreated()).andExpect(header().string("Idempotent-Replayed", "true"));
    }

    @Test void rejectsInvalidVinWithStableProblemDetails() throws Exception {
        mvc.perform(post("/api/v1/appointments").contentType("application/json").content(body("INVALID")))
            .andExpect(status().isBadRequest())
            .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
            .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"));
    }

    @Test void returnsAvailabilitySlots() throws Exception {
        mvc.perform(get("/api/v1/dealerships/{id}/availability",
                "11111111-1111-1111-1111-111111111112")
                .param("serviceTypeId", "44444444-4444-4444-4444-444444444401")
                .param("date", "2030-01-07"))
            .andExpect(status().isOk()).andExpect(jsonPath("$.availableStartTimes[0]").exists());
    }

    private String body(String vin) {
        return """
            {
              "dealershipId": "11111111-1111-1111-1111-111111111111",
              "customerId": "CUST-API",
              "vehicleVin": "%s",
              "serviceTypeId": "44444444-4444-4444-4444-444444444401",
              "desiredStartTime": "2030-01-07T09:00:00Z"
            }
            """.formatted(vin);
    }
}
```

- [ ] **Step 2: Run and verify failure**

```bash
./mvnw -Dtest=AppointmentApiTest test
```

Expected: FAIL because controllers are absent.

- [ ] **Step 3: Implement request validation and controllers**

Create `CreateAppointmentRequest.java`:

```java
package com.keyloop.scheduler.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.UUID;

public record CreateAppointmentRequest(
    @NotNull UUID dealershipId,
    @NotBlank String customerId,
    @NotBlank @Pattern(regexp = "^[A-HJ-NPR-Z0-9]{17}$") String vehicleVin,
    @NotNull UUID serviceTypeId,
    @NotNull Instant desiredStartTime) {}
```

Create `AppointmentController.java`:

```java
package com.keyloop.scheduler.api;

import com.keyloop.scheduler.booking.*;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/appointments")
public class AppointmentController {
    private final BookingService bookings;
    public AppointmentController(BookingService bookings) { this.bookings = bookings; }

    @PostMapping
    public ResponseEntity<Appointment> create(@Valid @RequestBody CreateAppointmentRequest request,
            @RequestHeader(name = "Idempotency-Key", required = false) String key) {
        BookingResult result = bookings.book(new BookingCommand(request.dealershipId(), request.customerId(),
            request.vehicleVin(), request.serviceTypeId(), request.desiredStartTime()), key);
        return ResponseEntity.created(URI.create("/api/v1/appointments/" + result.appointment().id()))
            .header("Idempotent-Replayed", Boolean.toString(result.replayed())).body(result.appointment());
    }

    @GetMapping("/{id}") public Appointment get(@PathVariable UUID id) { return bookings.get(id); }
    @PostMapping("/{id}/cancel") public Appointment cancel(@PathVariable UUID id) { return bookings.cancel(id); }
}
```

Create `AvailabilityController.java`:

```java
package com.keyloop.scheduler.api;

import com.keyloop.scheduler.availability.AvailabilityService;
import com.keyloop.scheduler.availability.DayAvailability;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import java.time.LocalDate;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dealerships")
public class AvailabilityController {
    private final AvailabilityService availability;
    public AvailabilityController(AvailabilityService availability) { this.availability = availability; }

    @GetMapping("/{id}/availability")
    public DayAvailability find(@PathVariable UUID id, @RequestParam UUID serviceTypeId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        return availability.find(id, serviceTypeId, date);
    }
}
```

- [ ] **Step 4: Implement RFC 9457 error mapping**

Create `ApiExceptionHandler.java`:

```java
package com.keyloop.scheduler.api;

import com.keyloop.scheduler.booking.BookingRejectedException;
import com.keyloop.scheduler.booking.RejectionReason;
import io.opentelemetry.api.trace.Span;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.util.stream.Collectors;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(BookingRejectedException.class)
    ResponseEntity<ProblemDetail> rejected(BookingRejectedException error) {
        return response(status(error.reason()), error.reason().name(), error.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> validation(MethodArgumentNotValidException error) {
        String detail = error.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage()).collect(Collectors.joining("; "));
        return response(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", detail);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<ProblemDetail> conversion(MethodArgumentTypeMismatchException error) {
        return response(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Invalid " + error.getName());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ProblemDetail> malformedJson(HttpMessageNotReadableException error) {
        return response(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Malformed JSON or invalid field value");
    }

    private ResponseEntity<ProblemDetail> response(HttpStatus status, String code, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(code.toLowerCase().replace('_', ' '));
        problem.setType(URI.create("https://api.keyloop.example/errors/" + code.toLowerCase().replace('_', '-')));
        problem.setProperty("errorCode", code);
        String traceId = Span.current().getSpanContext().getTraceId();
        if (!"00000000000000000000000000000000".equals(traceId)) problem.setProperty("traceId", traceId);
        return ResponseEntity.status(status).body(problem);
    }

    private HttpStatus status(RejectionReason reason) {
        return switch (reason) {
            case DEALERSHIP_NOT_FOUND, SERVICE_TYPE_NOT_FOUND, APPOINTMENT_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case NO_SERVICE_BAY_AVAILABLE, NO_QUALIFIED_TECHNICIAN_AVAILABLE,
                 VEHICLE_ALREADY_BOOKED, APPOINTMENT_ALREADY_CANCELLED -> HttpStatus.CONFLICT;
            case START_TIME_IN_PAST, OUTSIDE_OPERATING_HOURS, IDEMPOTENCY_KEY_REUSED -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
    }
}
```

- [ ] **Step 5: Run API and full service tests, then commit**

```bash
./mvnw -Dtest=AppointmentApiTest,BookingServiceIntegrationTest,ConcurrentBookingTest test
```

Expected: PASS; Swagger UI will expose the same controllers at `/swagger-ui.html` and `/v3/api-docs`.

```bash
git add src/main/java/com/keyloop/scheduler/api src/test/java/com/keyloop/scheduler/api
git commit -m "feat: expose appointment REST and OpenAPI contracts"
```

### Task 13: Finish metrics, JSON logs, tracing, and optional Grafana LGTM

**Files:**
- Modify: `pom.xml`
- Modify: `compose.yaml`
- Create: `src/main/java/com/keyloop/scheduler/booking/BookingMetrics.java`
- Modify: `src/main/java/com/keyloop/scheduler/booking/BookingService.java`
- Test: `src/test/java/com/keyloop/scheduler/api/ObservabilityApiTest.java`

- [ ] **Step 1: Write the failing metrics endpoint test**

Create `ObservabilityApiTest.java`:

```java
package com.keyloop.scheduler.api;

import com.keyloop.scheduler.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@AutoConfigureMockMvc
class ObservabilityApiTest extends IntegrationTestBase {
    @Autowired MockMvc mvc;

    @Test void exposesHealthAndSchedulerMetrics() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk())
            .andExpect(content().string(org.hamcrest.Matchers.containsString("scheduler_outbox_pending")));
    }
}
```

- [ ] **Step 2: Run and verify Prometheus is unavailable**

```bash
./mvnw -Dtest=ObservabilityApiTest test
```

Expected: FAIL because the Prometheus registry is absent.

- [ ] **Step 3: Add Prometheus and the optional LGTM container**

Add to `pom.xml`:

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

Add under `services:` in `compose.yaml`:

```yaml
  grafana:
    image: grafana/otel-lgtm:0.39.0
    profiles: [observability]
    ports:
      - "127.0.0.1:3000:3000"
      - "127.0.0.1:4318:4318"
```

- [ ] **Step 4: Instrument booking outcomes and latency**

Create `BookingMetrics.java`:

```java
package com.keyloop.scheduler.booking;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import java.util.function.Supplier;

@Component
public class BookingMetrics {
    private final MeterRegistry registry;
    public BookingMetrics(MeterRegistry registry) { this.registry = registry; }

    public <T> T measure(BookingCommand command, Supplier<T> action) {
        Timer.Sample sample = Timer.start(registry);
        try {
            T result = action.get();
            String outcome = result instanceof BookingResult booking && booking.replayed() ? "REPLAYED" : "CONFIRMED";
            registry.counter("scheduler.bookings", "outcome", outcome).increment();
            return result;
        } catch (BookingRejectedException e) {
            registry.counter("scheduler.bookings", "outcome", e.reason().name()).increment();
            throw e;
        } finally {
            sample.stop(registry.timer("scheduler.booking.duration"));
        }
    }
}
```

In `BookingService`, inject `BookingMetrics bookingMetrics` as the final constructor parameter and field. Rename the existing public `book` method to `private doBook`, then add this public entry point:

```java
public BookingResult book(BookingCommand command, String key) {
    return bookingMetrics.measure(command, () -> doBook(command, key));
}
```

Keep `doBook` unchanged. Add an SLF4J logger and log successful allocation after the transaction:

```java
private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(BookingService.class);
```

```java
log.info("Booking decision dealershipId={} serviceTypeCode={} outcome={} bayCode={} technicianId={}",
    dealership.id(), serviceType.code(), result.replayed() ? "REPLAYED" : "CONFIRMED",
    result.appointment().allocatedServiceBay().bayCode(), result.appointment().assignedTechnician().id());
```

- [ ] **Step 5: Run observability and regression tests, then commit**

```bash
./mvnw -Dtest=ObservabilityApiTest,BookingServiceIntegrationTest,OutboxRelayIntegrationTest test
```

Expected: PASS; `/actuator/prometheus` contains booking, outbox, cache, and JVM metrics. ECS stdout remains enabled in `application.yaml`; traces are sampled and exported only when an OTLP collector is running.

```bash
git add pom.xml compose.yaml src/main/java/com/keyloop/scheduler/booking src/test/java/com/keyloop/scheduler/api/ObservabilityApiTest.java
git commit -m "feat: add scheduler observability and local Grafana stack"
```

### Task 14: Add the reproducible demo, README, and final acceptance verification

**Files:**
- Create: `scripts/demo.sh`
- Create: `README.md`

- [ ] **Step 1: Create an executable end-to-end demo script**

Create `scripts/demo.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
DEALERSHIP_ID="11111111-1111-1111-1111-111111111112"
SERVICE_TYPE_ID="44444444-4444-4444-4444-444444444401"
START_TIME="${START_TIME:-2030-01-07T09:00:00Z}"
AVAILABILITY_DATE="${AVAILABILITY_DATE:-2030-01-07}"
KEY="demo-$(date +%s)"

payload=$(cat <<JSON
{
  "dealershipId": "${DEALERSHIP_ID}",
  "customerId": "CUST-DEMO",
  "vehicleVin": "WBA3A5C51CF256789",
  "serviceTypeId": "${SERVICE_TYPE_ID}",
  "desiredStartTime": "${START_TIME}"
}
JSON
)

echo "1. Create appointment"
created=$(curl -fsS -D /tmp/scheduler-create-headers.txt \
  -H "Content-Type: application/json" -H "Idempotency-Key: ${KEY}" \
  -d "${payload}" "${BASE_URL}/api/v1/appointments")
printf '%s\n' "${created}"
appointment_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"${created}")

echo "2. Replay the same request"
curl -fsS -H "Content-Type: application/json" -H "Idempotency-Key: ${KEY}" \
  -d "${payload}" "${BASE_URL}/api/v1/appointments"
printf '\n'

echo "3. Read appointment"
curl -fsS "${BASE_URL}/api/v1/appointments/${appointment_id}"
printf '\n'

echo "4. Read availability"
curl -fsS "${BASE_URL}/api/v1/dealerships/${DEALERSHIP_ID}/availability?serviceTypeId=${SERVICE_TYPE_ID}&date=${AVAILABILITY_DATE}"
printf '\n'

echo "5. Cancel appointment"
curl -fsS -X POST "${BASE_URL}/api/v1/appointments/${appointment_id}/cancel"
printf '\nAppointment ID: %s\n' "${appointment_id}"
```

Run `chmod +x scripts/demo.sh` after creating it. When executing after the seeded example date, set both `START_TIME` and `AVAILABILITY_DATE` to the same future Europe/London local date.

- [ ] **Step 2: Write the README with exact operating and verification instructions**

Create `README.md` with this content:

````markdown
# Unified Service Scheduler

Keyloop Technical Assessment — Scenario A (Ownership). Java 21 / Spring Boot backend for booking a vehicle service with automatic service-bay and qualified-technician allocation.

## Architecture and correctness

PostgreSQL is the source of truth. A per-dealership `pg_advisory_xact_lock` serializes allocation without blocking other dealerships. `btree_gist` exclusion constraints independently prevent overlapping confirmed appointments for a bay, technician, or VIN. Redis is fail-open caching only. Kafka receives at-least-once CloudEvents through a transactional outbox; Kafka availability never blocks booking.

See `docs/system-design.md` for assumptions, alternatives, data flows, API errors, reliability, and scalability.

## Prerequisites

- JDK 21 on `JAVA_HOME`
- Docker with Compose (Rancher Desktop works on macOS)
- No Maven installation; use `./mvnw`

## Run locally

```bash
docker compose up -d postgres redis kafka kafka-ui
./mvnw spring-boot:run
```

Optional observability:

```bash
docker compose --profile observability up -d grafana
```

Local URLs:

- Swagger UI: http://localhost:8080/swagger-ui.html
- OpenAPI JSON: http://localhost:8080/v3/api-docs
- Health: http://localhost:8080/actuator/health
- Prometheus: http://localhost:8080/actuator/prometheus
- Kafka UI: http://localhost:8081
- Grafana LGTM: http://localhost:3000

PostgreSQL is exposed only on localhost:5432, Redis on localhost:6380, and Kafka on localhost:9092. The Compose database trust configuration is only for this disposable local stack.

## Demo

With the application running:

```bash
./scripts/demo.sh
```

The script creates an appointment, repeats the same idempotent request, reads it, queries availability, and cancels it. In Kafka UI open `keyloop.ownership.appointments.v1` → Messages. Inspect timestamp, partition/offset, `appointmentId` key, `ce_type`, `ce_id`, `traceparent`, and the CloudEvents JSON body. The consumer group `notification-service-demo` demonstrates delivery and lag.

To use a different date, set `START_TIME` and `AVAILABILITY_DATE` to the same Europe/London local date.

## Tests

```bash
./mvnw clean verify
```

Tests use real PostgreSQL, Redis, and Kafka Testcontainers. Coverage includes pure interval/timezone/slot rules, physical DB constraints, all booking outcomes, idempotent replay, cancellation, versioned availability caching, CloudEvent publication, API Problem Details, and concurrency. The acceptance stress test sends 20 simultaneous requests to a one-bay dealership and requires exactly one booking plus 19 domain conflicts.

## AI collaboration narrative

Gemini produced design v1.1. Qoder performed an adversarial review and identified bay-lock false conflicts, snapshot races, Redis-only idempotency gaps, and outbox transaction scope. I reviewed the trade-offs and selected per-dealership advisory locks plus exclusion constraints, retained Redis/Kafka with narrower responsibilities, and used real-container tests as the acceptance gate. AI generated implementation candidates; compilation, automated tests, API/Kafka UI inspection, and this README record the verification evidence. The design document distinguishes implemented capabilities from designed-only extensions.

## 5–10 minute video outline

1. State the three booking requirements and show the architecture/correctness decision.
2. Create and replay a booking in Swagger UI; show allocated bay/technician and idempotent response.
3. Show the 20-thread test result and explain advisory locks plus exclusion constraints.
4. Open Kafka UI and inspect the confirmed/cancelled messages and consumer group.
5. Show availability changing after booking/cancellation and close with tests, metrics, limitations, and AI verification.
````

- [ ] **Step 3: Run the complete automated verification**

```bash
./mvnw clean verify
git diff --check
```

Expected: all unit, integration, concurrency, messaging, API, and observability tests PASS; no whitespace errors.

- [ ] **Step 4: Run the golden-path demo and inspect real UIs**

In terminal 1:

```bash
docker compose up -d postgres redis kafka kafka-ui
./mvnw spring-boot:run
```

In terminal 2:

```bash
./scripts/demo.sh
```

Verify manually:
- Swagger create returns `201`, a `Location` header, allocated bay and qualified technician.
- Replaying the same key/body returns the same appointment and `Idempotent-Replayed: true`.
- Same key/different body returns `422 IDEMPOTENCY_KEY_REUSED`; overlapping resource requests return `409`, never `500`.
- Availability excludes a booked slot and includes it after cancellation.
- Kafka UI shows confirmed and cancelled JSON messages with their actual timestamps, keys, headers, partitions, and offsets.
- `/actuator/health` is UP and `/actuator/prometheus` exposes scheduler metrics.

- [ ] **Step 5: Perform the post-implementation specification converge**

Read `docs/system-design.md` again and mark every item only after observing code/test/demo evidence:

```text
[x] Automatic ACTIVE bay and qualified ACTIVE technician allocation
[x] Full-duration half-open overlap rules and back-to-back bookings
[x] Dealership timezone, operating hours, past-time rejection, DST-safe conversion
[x] Customer reference and validated vehicle VIN persisted
[x] Per-dealership advisory lock and three GiST exclusion constraints
[x] Exactly one success for 20 concurrent requests against one bay
[x] No false conflict for adjacent slots; cross-dealership VIN race returns 409
[x] PostgreSQL-backed idempotency, Redis replay cache, same-body replay/different-body 422
[x] GET, cancel, and availability endpoints; cancellation frees resources
[x] Fail-open catalog and versioned availability caches
[x] Atomic appointment + outbox writes; Kafka publish outside DB transaction
[x] CloudEvents confirmed/cancelled, appointmentId key, ce_type/ce_id/traceparent headers
[x] Kafka UI and demo consumer; retry, stale claim, FAILED after ten attempts
[x] Swagger UI, RFC 9457 errors, health, Prometheus, ECS logs, tracing
[x] README, demo script, honest AI collaboration narrative, video outline
```

If any item is missing or partial, fix it and rerun `./mvnw clean verify`; do not report DONE with an unchecked requirement.

- [ ] **Step 6: Commit the delivery materials**

```bash
git add README.md scripts/demo.sh
git commit -m "docs: add verified scheduler demo and operating guide"
git status --short
```

Expected: clean working tree after the user-approved commit. Record the tested JDK/Docker versions and final test count for the submission/video.

