package com.keyloop.scheduler.events;

import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {"scheduler.events.enabled=true", "scheduler.events.poll-delay=100",
        "scheduler.events.initial-delay=100"})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaOutboxIntegrationTest {
    private static final String TOPIC = "keyloop.ownership.appointments.v1";
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("scheduler").withUsername("scheduler").withPassword("scheduler");
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.2");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired BookingService booking;
    @Autowired OutboxRelay relay;
    @Autowired JdbcClient jdbc;
    @Autowired JsonMapper mapper;
    @Autowired PlatformTransactionManager transactions;

    @BeforeEach
    void reset() {
        jdbc.sql("delete from outbox_events").update();
        jdbc.sql("delete from appointments").update();
    }

    @Test
    @Order(1)
    void refusesNetworkPublishingFromAnOuterDatabaseTransaction() {
        assertThatThrownBy(() -> new TransactionTemplate(transactions).executeWithoutResult(
                ignored -> relay.publishPending()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("outside a database transaction");
    }

    @Test
    @Order(2)
    void bookingIsAutomaticallyPublishedAsCloudEventAndOutboxBecomesPublished() {
        Appointment appointment = book();
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "acceptance-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        AtomicReference<ConsumerRecord<String, String>> received = new AtomicReference<>();
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            assertThat(consumer.partitionsFor(TOPIC)).hasSize(3);
            consumer.subscribe(List.of(TOPIC));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                for (var record : consumer.poll(Duration.ofMillis(300))) {
                    if (record.key().equals(appointment.id().toString())) {
                        received.set(record);
                    }
                }
                assertThat(received.get()).isNotNull();
            });
        }
        var record = received.get();
        var event = mapper.readTree(record.value());
        UUID eventId = jdbc.sql("select id from outbox_events where aggregate_id = :id")
                .param("id", appointment.id()).query(UUID.class).single();
        assertThat(record.key()).isEqualTo(appointment.id().toString());
        assertThat(new String(record.headers().lastHeader("ce_type").value(), StandardCharsets.UTF_8))
                .isEqualTo("com.keyloop.ownership.appointment.confirmed.v1");
        assertThat(new String(record.headers().lastHeader("ce_id").value(), StandardCharsets.UTF_8))
                .isEqualTo(eventId.toString());
        assertThat(event.path("specversion").asString()).isEqualTo("1.0");
        assertThat(event.path("id").asString()).isEqualTo(eventId.toString());
        assertThat(event.path("source").asString()).isEqualTo("/keyloop/ownership/unified-service-scheduler");
        assertThat(event.path("type").asString()).isEqualTo("com.keyloop.ownership.appointment.confirmed.v1");
        assertThat(event.path("subject").asString()).isEqualTo(appointment.id().toString());
        assertThat(event.path("time").asString()).isEqualTo(appointment.createdAt().toString());
        assertThat(event.path("datacontenttype").asString()).isEqualTo("application/json");
        var data = event.path("data");
        assertThat(data.path("appointmentId").asString()).isEqualTo(appointment.id().toString());
        assertThat(data.path("customerId").asString()).isEqualTo("CUST-KAFKA");
        assertThat(data.path("vehicleVin").asString()).isEqualTo("WBA3A5C51CF256789");
        assertThat(data.path("dealershipId").asString()).isEqualTo("11111111-1111-1111-1111-111111111111");
        assertThat(data.path("dealershipName").asString()).isEqualTo("Keyloop Central Motors");
        assertThat(data.path("serviceType").path("code").asString()).isEqualTo("OIL_CHANGE");
        assertThat(data.path("serviceType").path("durationMinutes").asInt()).isEqualTo(60);
        assertThat(data.path("serviceType").path("requiredSkill").asString()).isEqualTo("OIL_SERVICE");
        assertThat(data.path("allocatedServiceBay").path("id").asString())
                .isEqualTo(appointment.serviceBayId().toString());
        assertThat(data.path("assignedTechnician").path("id").asString())
                .isEqualTo(appointment.technicianId().toString());
        assertThat(data.path("startTime").asString()).isEqualTo("2030-01-07T09:00:00Z");
        assertThat(data.path("endTime").asString()).isEqualTo("2030-01-07T10:00:00Z");
        assertThat(data.path("status").asString()).isEqualTo("CONFIRMED");
        assertThat(data.path("createdAt").asString()).isEqualTo(appointment.createdAt().toString());
        assertThat(data.has("cancelledAt")).isTrue();
        assertThat(data.path("cancelledAt").isNull()).isTrue();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(jdbc.sql("select status from outbox_events where id = :id")
                        .param("id", eventId).query(String.class).single()).isEqualTo("PUBLISHED"));
    }

    @Test
    @Order(3)
    @ExtendWith(OutputCaptureExtension.class)
    void kafkaOutageDoesNotFailBookingAndLeavesDurablePendingEvent(CapturedOutput output) {
        KAFKA.stop();
        Appointment appointment = book();
        UUID eventId = jdbc.sql("select id from outbox_events where aggregate_id = :id")
                .param("id", appointment.id()).query(UUID.class).single();
        assertThat(booking.get(appointment.id())).isEqualTo(appointment);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(jdbc.sql("select status from outbox_events where id = :id")
                    .param("id", eventId).query(String.class).single()).isEqualTo("PENDING");
            assertThat(jdbc.sql("select attempts from outbox_events where id = :id")
                    .param("id", eventId).query(Integer.class).single()).isGreaterThanOrEqualTo(1);
        });
        assertThat(output.toString()).doesNotContain("payload='", "CUST-KAFKA", "WBA3A5C51CF256789");
    }

    private Appointment book() {
        return booking.book(new BookingCommand(UUID.fromString("11111111-1111-1111-1111-111111111111"),
                "CUST-KAFKA", "WBA3A5C51CF256789", UUID.fromString("44444444-4444-4444-4444-444444444401"),
                Instant.parse("2030-01-07T09:00:00Z")));
    }
}
