package com.keyloop.scheduler.events;

import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
class OutboxIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("scheduler").withUsername("scheduler").withPassword("scheduler");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired BookingService booking;
    @Autowired OutboxRepository outbox;
    @Autowired JdbcClient jdbc;

    @BeforeEach
    void reset() {
        jdbc.sql("delete from outbox_events").update();
        jdbc.sql("delete from appointments").update();
    }

    @Test
    void failedOutboxInsertRollsBackAppointment() {
        jdbc.sql("""
                create function reject_test_outbox() returns trigger language plpgsql as $$
                begin raise exception 'outbox insert failure' using errcode = '23514'; end; $$
                """).update();
        jdbc.sql("""
                create trigger reject_outbox before insert on outbox_events
                for each row execute function reject_test_outbox()
                """).update();
        try {
            assertThatThrownBy(() -> book("WBA3A5C51CF256789")).isInstanceOf(RuntimeException.class);
            assertThat(count("appointments")).isZero();
            assertThat(count("outbox_events")).isZero();
        } finally {
            jdbc.sql("drop trigger reject_outbox on outbox_events").update();
            jdbc.sql("drop function reject_test_outbox()").update();
        }
    }

    @Test
    void staleLeaseIsReclaimedAndOldWorkerCannotCompleteIt() {
        book("WBA3A5C51CF256789");
        var original = outbox.claim(1).getFirst();
        assertThat(outbox.claim(1)).isEmpty();
        jdbc.sql("update outbox_events set claimed_at = current_timestamp - interval '31 seconds'").update();

        var replacement = outbox.claim(1).getFirst();
        assertThat(replacement.id()).isEqualTo(original.id());
        assertThat(replacement.claimToken()).isNotEqualTo(original.claimToken());
        assertThat(replacement.attempts()).isEqualTo(2);
        assertThat(outbox.markPublished(original)).isFalse();
        assertThat(outbox.markFailed(original, "OldWorker")).isFalse();
        assertThat(outbox.markPublished(replacement)).isTrue();
        assertThat(state()).isEqualTo("PUBLISHED");
    }

    @Test
    void failedDeliveryKeepsEventIdAndPayloadAndStopsAfterTenAttempts() {
        book("WBA3A5C51CF256789");
        UUID id = jdbc.sql("select id from outbox_events").query(UUID.class).single();
        String payload = jdbc.sql("select payload::text from outbox_events").query(String.class).single();
        for (int attempt = 1; attempt <= 10; attempt++) {
            var claimed = outbox.claim(1).getFirst();
            assertThat(claimed.id()).isEqualTo(id);
            assertThat(claimed.payload()).isEqualTo(payload);
            assertThat(claimed.attempts()).isEqualTo(attempt);
            assertThat(outbox.markFailed(claimed, "BrokerUnavailable")).isTrue();
            assertThat(state()).isEqualTo(attempt < 10 ? "PENDING" : "FAILED");
            jdbc.sql("update outbox_events set next_attempt_at = current_timestamp - interval '1 second'").update();
        }
        assertThat(outbox.claim(1)).isEmpty();
        assertThat(jdbc.sql("select attempts from outbox_events").query(Integer.class).single()).isEqualTo(10);
    }

    @Test
    void expiredFinalAttemptIsFailedWithoutAnEleventhClaim() {
        book("WBA3A5C51CF256789");
        outbox.claim(1);
        jdbc.sql("""
                update outbox_events set attempts = 10,
                    claimed_at = current_timestamp - interval '31 seconds'
                """).update();

        assertThat(outbox.claim(1)).isEmpty();
        assertThat(state()).isEqualTo("FAILED");
    }

    @Test
    void concurrentRelaysClaimDifferentRows() throws Exception {
        book("WBA3A5C51CF256789");
        book("1HGCM82633A004352");
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> outbox.claim(1));
            var second = workers.submit(() -> outbox.claim(1));
            assertThat(first.get(10, TimeUnit.SECONDS).getFirst().id())
                    .isNotEqualTo(second.get(10, TimeUnit.SECONDS).getFirst().id());
        }
        assertThat(jdbc.sql("select count(*) from outbox_events where status = 'IN_FLIGHT'")
                .query(Long.class).single()).isEqualTo(2);
    }

    private Appointment book(String vin) {
        return booking.book(new BookingCommand(UUID.fromString("11111111-1111-1111-1111-111111111111"),
                "CUST-OUTBOX", vin, UUID.fromString("44444444-4444-4444-4444-444444444401"),
                Instant.parse("2030-01-07T09:00:00Z")));
    }

    private long count(String table) {
        return jdbc.sql("select count(*) from " + table).query(Long.class).single();
    }

    private String state() {
        return jdbc.sql("select status from outbox_events").query(String.class).single();
    }
}
