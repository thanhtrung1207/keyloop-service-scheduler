package com.keyloop.scheduler.catalog;

import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "scheduler.cache.enabled=true")
@Testcontainers
class CatalogCacheIntegrationTest {
    private static final UUID DEALER = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID SERVICE = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final String DEALER_KEY = "scheduler:catalog:dealership:" + DEALER;
    private static final String SERVICE_KEY = "scheduler:catalog:service-type:" + SERVICE;
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("scheduler").withUsername("scheduler").withPassword("scheduler");
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
        properties.add("spring.data.redis.host", REDIS::getHost);
        properties.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired BookingService booking;
    @Autowired StringRedisTemplate redis;
    @Autowired JdbcClient jdbc;

    @BeforeEach
    void reset() {
        jdbc.sql("delete from outbox_events").update();
        jdbc.sql("delete from appointments").update();
        jdbc.sql("update dealerships set name = 'Keyloop Central Motors'").update();
        jdbc.sql("update service_types set name = 'Oil change' where id = :id").param("id", SERVICE).update();
        redis.delete(java.util.List.of(DEALER_KEY, SERVICE_KEY));
    }

    @Test
    void bookingPopulatesIndependentCatalogKeysForOneHour() {
        book("WBA3A5C51CF256789", "2030-01-07T09:00:00Z");

        assertThat(redis.opsForValue().get(DEALER_KEY)).contains("Europe/London");
        assertThat(redis.opsForValue().get(SERVICE_KEY)).contains("OIL_SERVICE");
        assertThat(redis.getExpire(DEALER_KEY, TimeUnit.SECONDS)).isBetween(3590L, 3600L);
        assertThat(redis.getExpire(SERVICE_KEY, TimeUnit.SECONDS)).isBetween(3590L, 3600L);
    }

    @Test
    void metadataCacheHitAvoidsReadingChangedDatabaseCatalogUntilExpiry() {
        book("WBA3A5C51CF256789", "2030-01-07T09:00:00Z");
        jdbc.sql("update dealerships set name = 'Renamed dealership'").update();
        jdbc.sql("update service_types set name = 'Renamed service' where id = :id").param("id", SERVICE).update();

        Appointment next = book("1HGCM82633A004352", "2030-01-07T11:00:00Z");
        assertThat(next.dealershipName()).isEqualTo("Keyloop Central Motors");
        assertThat(next.serviceTypeName()).isEqualTo("Oil change");
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-json", "{}", "null"})
    void invalidOrIncompleteCachedCatalogFallsBackToDatabase(String corruptValue) {
        redis.opsForValue().set(DEALER_KEY, corruptValue);
        redis.opsForValue().set(SERVICE_KEY, corruptValue);

        Appointment appointment = book("WBA3A5C51CF256789", "2030-01-07T09:00:00Z");
        assertThat(appointment.dealershipName()).isEqualTo("Keyloop Central Motors");
        assertThat(appointment.serviceTypeName()).isEqualTo("Oil change");
        assertThat(redis.opsForValue().get(DEALER_KEY)).contains("Europe/London");
    }

    private Appointment book(String vin, String start) {
        return booking.book(new BookingCommand(DEALER, "CUST-REDIS", vin, SERVICE, Instant.parse(start)));
    }
}
