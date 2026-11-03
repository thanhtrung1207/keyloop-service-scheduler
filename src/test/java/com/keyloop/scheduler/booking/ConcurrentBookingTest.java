package com.keyloop.scheduler.booking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class ConcurrentBookingTest {
    private static final String DEALERSHIP_ID = "11111111-1111-1111-1111-111111111111";
    private static final String OIL_SERVICE_ID = "44444444-4444-4444-4444-444444444401";
    private static final String SECOND_DEALERSHIP_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private static final Instant START = Instant.parse("2030-01-07T09:00:00Z");
    private static final int REQUEST_COUNT = 12;

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler");

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    BookingService bookingService;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void resetDemoState() {
        jdbc.sql("delete from appointments").update();
        jdbc.sql("update service_bays set status = 'ACTIVE'").update();
        jdbc.sql("update technicians set status = 'ACTIVE'").update();
    }

    @Test
    void serializesConcurrentAllocationAndUsesBothAvailableResourcePairs()
            throws InterruptedException, ExecutionException, TimeoutException {
        installShortInsertDelay(true);

        ExecutorService executor = Executors.newFixedThreadPool(REQUEST_COUNT);
        CountDownLatch ready = new CountDownLatch(REQUEST_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<RejectionReason>> outcomes = new ArrayList<>();
        try {
            for (int i = 0; i < REQUEST_COUNT; i++) {
                final int requestNumber = i;
                outcomes.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Concurrent booking start was not released");
                    }
                    try {
                        bookingService.book(new BookingCommand(
                                java.util.UUID.fromString(DEALERSHIP_ID),
                                "CUST-CONCURRENT-" + requestNumber,
                                String.format("WBA3A5C51CF%06d", requestNumber),
                                java.util.UUID.fromString(OIL_SERVICE_ID),
                                START));
                        return null;
                    } catch (BookingRejectedException rejected) {
                        return rejected.reason();
                    }
                }));
            }

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            int successes = 0;
            int conflicts = 0;
            for (Future<RejectionReason> outcome : outcomes) {
                RejectionReason rejection = outcome.get(20, TimeUnit.SECONDS);
                if (rejection == null) {
                    successes++;
                } else {
                    assertThat(rejection).isIn(RejectionReason.NO_SERVICE_BAY_AVAILABLE,
                            RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE);
                    conflicts++;
                }
            }

            assertThat(successes).isEqualTo(2);
            assertThat(conflicts).isEqualTo(REQUEST_COUNT - 2);
            assertThat(count("appointments")).isEqualTo(2);
            assertThat(countDistinct("service_bay_id")).isEqualTo(2);
            assertThat(countDistinct("technician_id")).isEqualTo(2);
        } finally {
            start.countDown();
            executor.shutdownNow();
            restoreResourceConstraints();
        }
    }

    @Test
    void withOneAvailableResourceOnlyOneConcurrentRequestSucceedsWithConstraintsIntact()
            throws InterruptedException, ExecutionException, TimeoutException {
        jdbc.sql("update service_bays set status = 'MAINTENANCE' where bay_code = 'BAY-02'").update();
        assertThat(jdbc.sql("update technicians set status = 'INACTIVE' where id = :id")
                .param("id", java.util.UUID.fromString("33333333-3333-3333-3333-333333333301"))
                .update()).isEqualTo(1);
        assertThat(jdbc.sql("""
                select count(*) from technicians t
                join technician_skills s on s.technician_id = t.id
                where t.dealership_id = :id and t.status = 'ACTIVE' and s.skill_code = 'OIL_SERVICE'
                """).param("id", java.util.UUID.fromString(DEALERSHIP_ID)).query(Long.class).single())
                .isEqualTo(1);
        installShortInsertDelay(false);

        List<RejectionReason> outcomes = runConcurrentRequests(REQUEST_COUNT, requestNumber ->
                new BookingCommand(java.util.UUID.fromString(DEALERSHIP_ID),
                        "CUST-ONE-RESOURCE-" + requestNumber,
                        String.format("WBA3A5C51CF%06d", requestNumber),
                        java.util.UUID.fromString(OIL_SERVICE_ID), START));

        assertThat(outcomes.stream().filter(reason -> reason == null)).hasSize(1);
        assertThat(outcomes.stream().filter(reason -> reason != null))
                .containsOnly(RejectionReason.NO_SERVICE_BAY_AVAILABLE);
        assertThat(count("appointments")).isEqualTo(1);
    }

    @Test
    void databaseVinConstraintRejectsConcurrentSameVehicleAcrossDealerships()
            throws InterruptedException, ExecutionException, TimeoutException {
        createSecondDealership();
        installShortInsertDelay(false);
        String vin = "WBA3A5C51CF256789";

        List<RejectionReason> outcomes = runConcurrentRequests(2, requestNumber ->
                new BookingCommand(java.util.UUID.fromString(
                        requestNumber == 0 ? DEALERSHIP_ID : SECOND_DEALERSHIP_ID),
                        "CUST-CROSS-DEALER-" + requestNumber, vin,
                        java.util.UUID.fromString(OIL_SERVICE_ID), START));

        assertThat(outcomes.stream().filter(reason -> reason == null)).hasSize(1);
        assertThat(outcomes.stream().filter(reason -> reason != null))
                .containsOnly(RejectionReason.VEHICLE_ALREADY_BOOKED);
        assertThat(count("appointments")).isEqualTo(1);
    }

    private List<RejectionReason> runConcurrentRequests(int requestCount,
                                                          java.util.function.IntFunction<BookingCommand> commands)
            throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(requestCount);
        CountDownLatch ready = new CountDownLatch(requestCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<RejectionReason>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < requestCount; i++) {
                final int requestNumber = i;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Concurrent booking start was not released");
                    }
                    try {
                        bookingService.book(commands.apply(requestNumber));
                        return null;
                    } catch (BookingRejectedException rejected) {
                        return rejected.reason();
                    }
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            List<RejectionReason> outcomes = new ArrayList<>();
            for (Future<RejectionReason> future : futures) {
                outcomes.add(future.get(20, TimeUnit.SECONDS));
            }
            return outcomes;
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    private void installShortInsertDelay(boolean disableResourceConstraints) {
        if (disableResourceConstraints) {
            // The two-resource race isolates the dealership lock; production constraints are tested below.
            jdbc.sql("alter table appointments drop constraint appointments_bay_no_overlap").update();
            jdbc.sql("alter table appointments drop constraint appointments_technician_no_overlap").update();
        }
        jdbc.sql("drop trigger if exists scheduler_test_insert_delay on appointments").update();
        jdbc.sql("""
                create or replace function delay_scheduler_appointment_insert()
                returns trigger language plpgsql as $$
                begin
                    perform pg_sleep(0.75);
                    return new;
                end;
                $$
                """).update();
        jdbc.sql("""
                create trigger scheduler_test_insert_delay
                before insert on appointments
                for each row execute function delay_scheduler_appointment_insert()
                """).update();
    }

    private void restoreResourceConstraints() {
        jdbc.sql("""
                alter table appointments add constraint appointments_bay_no_overlap exclude using gist
                    (service_bay_id with =, tstzrange(start_time, end_time, '[)') with &&)
                """).update();
        jdbc.sql("""
                alter table appointments add constraint appointments_technician_no_overlap exclude using gist
                    (technician_id with =, tstzrange(start_time, end_time, '[)') with &&)
                """).update();
    }

    private void createSecondDealership() {
        jdbc.sql("""
                insert into dealerships (id, name, timezone, open_time, close_time)
                values (:id, 'Second Dealer', 'UTC', :openTime, :closeTime)
                on conflict (id) do nothing
                """)
                .param("id", java.util.UUID.fromString(SECOND_DEALERSHIP_ID))
                .param("openTime", LocalTime.of(8, 0))
                .param("closeTime", LocalTime.of(18, 0))
                .update();
        jdbc.sql("""
                insert into service_bays (id, dealership_id, bay_code, status)
                values ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', :dealershipId, 'BAY-SECOND', 'ACTIVE')
                on conflict (id) do nothing
                """)
                .param("dealershipId", java.util.UUID.fromString(SECOND_DEALERSHIP_ID))
                .update();
        jdbc.sql("""
                insert into technicians (id, dealership_id, full_name, status)
                values ('cccccccc-cccc-cccc-cccc-cccccccccccc', :dealershipId, 'Taylor Tech', 'ACTIVE')
                on conflict (id) do nothing
                """)
                .param("dealershipId", java.util.UUID.fromString(SECOND_DEALERSHIP_ID))
                .update();
        jdbc.sql("""
                insert into technician_skills (technician_id, skill_code)
                values ('cccccccc-cccc-cccc-cccc-cccccccccccc', 'OIL_SERVICE')
                on conflict do nothing
                """).update();
    }

    private long count(String table) {
        return jdbc.sql("select count(*) from " + table).query(Long.class).single();
    }

    private long countDistinct(String column) {
        return jdbc.sql("select count(distinct " + column + ") from appointments")
                .query(Long.class)
                .single();
    }
}
