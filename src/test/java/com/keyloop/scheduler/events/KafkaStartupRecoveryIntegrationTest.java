package com.keyloop.scheduler.events;

import com.keyloop.scheduler.ServiceSchedulerApplication;
import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class KafkaStartupRecoveryIntegrationTest {
    private static final String TOPIC = "keyloop.ownership.appointments.v1";
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("scheduler").withUsername("scheduler").withPassword("scheduler");
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.2");

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void appStartsAndBooksBeforeFreshBrokerRecoversThenPublishesToThreePartitions(CapturedOutput output) throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        String containerId = KAFKA.getContainerId();
        var docker = DockerClientFactory.instance().client();
        // Make this test's fresh broker unresponsive without changing its published port.
        // Docker stop/start can reassign an automatically allocated host port.
        docker.pauseContainerCmd(containerId).exec();
        try (var context = new SpringApplicationBuilder(ServiceSchedulerApplication.class)
                .web(WebApplicationType.NONE).run(
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.kafka.bootstrap-servers=" + bootstrap,
                        "--logging.level.org.apache.kafka=ERROR",
                        "--scheduler.events.enabled=true", "--scheduler.cache.enabled=false",
                        "--scheduler.events.initial-delay=100", "--scheduler.events.poll-delay=100")) {
            var booking = context.getBean(BookingService.class);
            var jdbc = context.getBean(JdbcClient.class);
            var appointment = booking.book(new BookingCommand(
                    UUID.fromString("11111111-1111-1111-1111-111111111111"), "CUST-STARTUP",
                    "WBA3A5C51CF256789", UUID.fromString("44444444-4444-4444-4444-444444444401"),
                    Instant.parse("2030-01-07T09:00:00Z")));
            assertThat(booking.get(appointment.id()).status()).isEqualTo("CONFIRMED");
            assertThat(jdbc.sql("select count(*) from outbox_events").query(Long.class).single()).isEqualTo(1);
            await().atMost(Duration.ofSeconds(12)).until(() -> output.toString().contains("kafka_topic_retry"));
            assertThat(jdbc.sql("select status from outbox_events").query(String.class).single()).isEqualTo("PENDING");

            docker.unpauseContainerCmd(containerId).exec();
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jdbc.sql("select status from outbox_events").query(String.class).single())
                            .isEqualTo("PUBLISHED"));
            try (var admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
                assertThat(admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS)
                        .get(TOPIC).partitions()).hasSize(3);
            }

            Properties config = new Properties();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "startup-acceptance-" + UUID.randomUUID());
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            try (var consumer = new KafkaConsumer<String, String>(config)) {
                consumer.subscribe(List.of(TOPIC));
                await().atMost(Duration.ofSeconds(20)).until(() -> {
                    for (var record : consumer.poll(Duration.ofMillis(300))) {
                        if (appointment.id().toString().equals(record.key())) {
                            return true;
                        }
                    }
                    return false;
                });
            }
        } finally {
            if (Boolean.TRUE.equals(docker.inspectContainerCmd(containerId).exec().getState().getPaused())) {
                docker.unpauseContainerCmd(containerId).exec();
            }
        }
    }
}
