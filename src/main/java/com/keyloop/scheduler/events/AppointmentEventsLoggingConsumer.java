package com.keyloop.scheduler.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@ConditionalOnProperty(name = "scheduler.events.enabled", havingValue = "true")
public class AppointmentEventsLoggingConsumer {
    private static final Logger log = LoggerFactory.getLogger(AppointmentEventsLoggingConsumer.class);

    @KafkaListener(topics = "${scheduler.events.topic}", groupId = "notification-service-demo")
    public void receive(ConsumerRecord<String, String> record) {
        var eventId = record.headers().lastHeader("ce_id");
        log.info("demo_event_received eventId={} appointmentId={} partition={} offset={}",
                eventId == null ? "missing" : new String(eventId.value(), StandardCharsets.UTF_8),
                record.key(), record.partition(), record.offset());
    }
}
