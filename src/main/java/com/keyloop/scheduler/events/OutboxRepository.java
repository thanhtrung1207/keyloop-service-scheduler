package com.keyloop.scheduler.events;

import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.catalog.ServiceType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxRepository {
    public record ClaimedEvent(UUID id, UUID aggregateId, String eventType, String payload,
                               int attempts, UUID claimToken) { }

    private final JdbcClient jdbc;
    private final JsonMapper mapper;
    private final Clock clock;

    public OutboxRepository(JdbcClient jdbc, JsonMapper mapper, Clock clock) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.clock = clock;
    }

    // MANDATORY prevents accidental non-atomic use outside the booking transaction.
    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.MANDATORY)
    public void enqueue(Appointment appointment, ServiceType serviceType) {
        UUID eventId = UUID.randomUUID();
        String payload = mapper.writeValueAsString(AppointmentConfirmedEvent.from(eventId, appointment, serviceType));
        jdbc.sql("""
                insert into outbox_events (id, aggregate_id, event_type, payload)
                values (:id, :aggregateId, :eventType, cast(:payload as jsonb))
                """)
                .param("id", eventId).param("aggregateId", appointment.id())
                .param("eventType", AppointmentConfirmedEvent.TYPE).param("payload", payload).update();
    }

    @Transactional
    public List<ClaimedEvent> claim(int batchSize) {
        if (batchSize < 1 || batchSize > 50) {
            throw new IllegalArgumentException("batch size must be between 1 and 50");
        }
        Instant now = clock.instant();
        var expired = now.minusSeconds(30).atOffset(ZoneOffset.UTC);
        jdbc.sql("""
                update outbox_events set status = 'FAILED', claim_token = null,
                    last_error = 'Final publishing lease expired'
                where status = 'IN_FLIGHT' and claimed_at <= :expired and attempts >= 10
                """).param("expired", expired).update();
        UUID claimToken = UUID.randomUUID();
        return jdbc.sql("""
                update outbox_events set status = 'IN_FLIGHT', attempts = attempts + 1,
                    claimed_at = :now, claim_token = :token
                where id in (
                    select id from outbox_events
                    where attempts < 10 and (
                        (status = 'PENDING' and next_attempt_at <= :now)
                        or (status = 'IN_FLIGHT' and claimed_at <= :expired)
                    )
                    order by created_at, id
                    for update skip locked limit :batchSize
                )
                returning id, aggregate_id, event_type, payload::text, attempts, claim_token
                """)
                .param("now", now.atOffset(ZoneOffset.UTC)).param("expired", expired)
                .param("token", claimToken).param("batchSize", batchSize)
                .query((rs, row) -> new ClaimedEvent(rs.getObject("id", UUID.class),
                        rs.getObject("aggregate_id", UUID.class), rs.getString("event_type"),
                        rs.getString("payload"), rs.getInt("attempts"), rs.getObject("claim_token", UUID.class)))
                .list();
    }

    @Transactional
    public boolean markPublished(ClaimedEvent event) {
        return jdbc.sql("""
                update outbox_events set status = 'PUBLISHED', published_at = :now,
                    claim_token = null, last_error = null
                where id = :id and status = 'IN_FLIGHT' and claim_token = :token
                """).param("now", clock.instant().atOffset(ZoneOffset.UTC))
                .param("id", event.id()).param("token", event.claimToken()).update() == 1;
    }

    @Transactional
    public boolean markFailed(ClaimedEvent event, String failureType) {
        long delaySeconds = Math.min(30, 1L << (event.attempts() - 1));
        String safeError = failureType.substring(0, Math.min(512, failureType.length()));
        return jdbc.sql("""
                update outbox_events set status = case when attempts >= 10 then 'FAILED' else 'PENDING' end,
                    next_attempt_at = :nextAttempt, last_error = :error, claim_token = null
                where id = :id and status = 'IN_FLIGHT' and claim_token = :token
                """).param("nextAttempt", clock.instant().plusSeconds(delaySeconds).atOffset(ZoneOffset.UTC))
                .param("error", safeError).param("id", event.id()).param("token", event.claimToken()).update() == 1;
    }
}
