package com.keyloop.scheduler.catalog;

import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {"scheduler.cache.enabled=true", "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=1", "spring.data.redis.connect-timeout=100ms", "spring.data.redis.timeout=100ms"})
@Testcontainers
class RedisUnavailableBookingIntegrationTest {
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
    @Autowired StringRedisTemplate redis;

    @Test
    void refusedRedisConnectionDoesNotPreventConfirmedBooking() {
        assertThatThrownBy(() -> redis.opsForValue().get("probe")).isInstanceOf(RuntimeException.class);

        var appointment = booking.book(new BookingCommand(
                UUID.fromString("11111111-1111-1111-1111-111111111111"), "CUST-FALLBACK",
                "WBA3A5C51CF256789", UUID.fromString("44444444-4444-4444-4444-444444444401"),
                Instant.parse("2030-01-07T09:00:00Z")));

        assertThat(appointment.status()).isEqualTo("CONFIRMED");
        assertThat(booking.get(appointment.id())).isEqualTo(appointment);
    }
}
