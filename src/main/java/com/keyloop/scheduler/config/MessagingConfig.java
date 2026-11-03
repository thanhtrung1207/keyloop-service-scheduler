package com.keyloop.scheduler.config;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableKafka
@ConditionalOnProperty(name = "scheduler.events.enabled", havingValue = "true")
public class MessagingConfig {
    private static final Logger log = LoggerFactory.getLogger(MessagingConfig.class);

    @Bean
    ProducerListener<Object, Object> kafkaProducerListener() {
        return new ProducerListener<>() {
            @Override
            public void onError(ProducerRecord<Object, Object> record, RecordMetadata metadata, Exception error) {
                // Kafka's default listener includes event payloads, which contain customer/VIN data.
                log.warn("kafka_send_failed appointmentId={} failureType={}",
                        record.key(), error.getClass().getSimpleName());
            }
        };
    }

}
