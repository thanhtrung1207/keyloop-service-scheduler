package com.keyloop.scheduler.events;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "scheduler.events.enabled", havingValue = "true")
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private final KafkaTopicProvisioner topics;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka,
                       @Value("${scheduler.events.topic}") String topic, KafkaTopicProvisioner topics) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.topic = topic;
        this.topics = topics;
    }

    @Scheduled(fixedDelayString = "${scheduler.events.poll-delay:500}",
            initialDelayString = "${scheduler.events.initial-delay:1000}")
    public void publishPending() {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox publishing must run outside a database transaction");
        }
        if (!topics.ensureReady()) {
            return;
        }
        // Claim one at a time so earlier network sends cannot consume a later row's lease.
        for (int i = 0; i < 10; i++) {
            var rows = outbox.claim(1);
            if (rows.isEmpty()) {
                return;
            }
            var event = rows.getFirst();
            try {
                var record = new ProducerRecord<String, String>(topic, event.aggregateId().toString(), event.payload());
                record.headers().add("ce_type", event.eventType().getBytes(StandardCharsets.UTF_8));
                record.headers().add("ce_id", event.id().toString().getBytes(StandardCharsets.UTF_8));
                kafka.send(record).get(5, TimeUnit.SECONDS);
                if (outbox.markPublished(event)) {
                    log.info("outbox_published eventId={} appointmentId={} attempts={}",
                            event.id(), event.aggregateId(), event.attempts());
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                outbox.markFailed(event, "InterruptedException");
                return;
            } catch (Exception failure) {
                topics.invalidate();
                outbox.markFailed(event, failure.getClass().getSimpleName());
                log.warn("outbox_retry eventId={} attempts={} failureType={}",
                        event.id(), event.attempts(), failure.getClass().getSimpleName());
                return;
            }
        }
    }
}
