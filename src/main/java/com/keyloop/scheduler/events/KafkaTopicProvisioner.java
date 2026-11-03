package com.keyloop.scheduler.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "scheduler.events.enabled", havingValue = "true")
public class KafkaTopicProvisioner {
    private static final Logger log = LoggerFactory.getLogger(KafkaTopicProvisioner.class);
    private final KafkaAdmin admin;
    private final String topic;
    private boolean ready;
    private long retryAtNanos;

    public KafkaTopicProvisioner(KafkaAdmin admin, @Value("${scheduler.events.topic}") String topic) {
        this.admin = admin;
        this.topic = topic;
        // Provision on the relay thread after startup; broker availability must not gate HTTP startup.
        admin.setAutoCreate(false);
        admin.setCloseTimeout(1);
        admin.setOperationTimeout(3);
    }

    public synchronized boolean ensureReady() {
        if (ready) {
            return true;
        }
        if (retryAtNanos != 0 && System.nanoTime() - retryAtNanos < 0) {
            return false;
        }
        try {
            admin.createOrModifyTopics(TopicBuilder.name(topic).partitions(3).replicas(1).build());
            ready = true;
            retryAtNanos = 0;
            log.info("kafka_topic_ready topic={} partitions=3", topic);
            return true;
        } catch (RuntimeException failure) {
            retryAtNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            log.warn("kafka_topic_retry topic={} failureType={}", topic, failure.getClass().getSimpleName());
            return false;
        }
    }

    public synchronized void invalidate() {
        ready = false;
    }
}
