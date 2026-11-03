package com.keyloop.scheduler.booking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
class BookingRepositoryIntegrationTest {
    private static final UUID DEALERSHIP_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID OIL_SERVICE_ID = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final UUID BAY_ONE = UUID.fromString("22222222-2222-2222-2222-222222222201");
    private static final UUID BAY_TWO = UUID.fromString("22222222-2222-2222-2222-222222222202");
    private static final UUID ALEX = UUID.fromString("33333333-3333-3333-3333-333333333301");
    private static final UUID SAM = UUID.fromString("33333333-3333-3333-3333-333333333302");
    private static final Instant NINE = Instant.parse("2030-01-07T09:00:00Z");
    private static final Instant TEN = Instant.parse("2030-01-07T10:00:00Z");
    private static final Instant ELEVEN = Instant.parse("2030-01-07T11:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("scheduler")
            .withUsername("scheduler")
            .withPassword("scheduler");

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    BookingRepository repository;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void clearAppointments() {
        jdbc.sql("delete from appointments").update();
    }

    @Test
    void selectsFirstFreeBayAndLeastSkilledQualifiedTechnician() {
        TimeSlot slot = new TimeSlot(NINE, TEN);

        assertThat(repository.findFreeBay(DEALERSHIP_ID, slot).orElseThrow().bayCode()).isEqualTo("BAY-01");
        assertThat(repository.findFreeTechnician(DEALERSHIP_ID, "OIL_SERVICE", slot)
                .orElseThrow().fullName()).isEqualTo("Sam Reed");
    }

    @Test
    void excludesResourcesBusyForTheWholeSlotAndFindsVinOverlap() {
        String vin = "WBA3A5C51CF256789";
        insertAppointment(UUID.randomUUID(), BAY_ONE, SAM, vin, NINE, TEN);
        TimeSlot overlapping = new TimeSlot(Instant.parse("2030-01-07T09:30:00Z"), ELEVEN);

        assertThat(repository.hasVinOverlap(vin, overlapping)).isTrue();
        assertThat(repository.findFreeBay(DEALERSHIP_ID, overlapping).orElseThrow().id()).isEqualTo(BAY_TWO);
        assertThat(repository.findFreeTechnician(DEALERSHIP_ID, "OIL_SERVICE", overlapping)
                .orElseThrow().id()).isEqualTo(ALEX);
    }

    @Test
    void allowsAdjacentResourceAndVinIntervals() {
        String vin = "WBA3A5C51CF256789";
        insertAppointment(UUID.randomUUID(), BAY_ONE, SAM, vin, NINE, TEN);
        TimeSlot adjacent = new TimeSlot(TEN, ELEVEN);

        assertThat(repository.hasVinOverlap(vin, adjacent)).isFalse();
        assertThat(repository.findFreeBay(DEALERSHIP_ID, adjacent).orElseThrow().id()).isEqualTo(BAY_ONE);
        assertThat(repository.findFreeTechnician(DEALERSHIP_ID, "OIL_SERVICE", adjacent)
                .orElseThrow().id()).isEqualTo(SAM);
    }

    @Test
    void databaseRejectsOverlappingConfirmedAppointmentsForSameBay() {
        String vinOne = "WBA3A5C51CF256789";
        insertAppointment(UUID.randomUUID(), BAY_ONE, ALEX, vinOne, NINE, TEN);

        assertThatThrownBy(() -> insertAppointment(UUID.randomUUID(), BAY_ONE, SAM,
                "1HGCM82633A004352", Instant.parse("2030-01-07T09:30:00Z"), ELEVEN))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private void insertAppointment(UUID id, UUID bay, UUID technician, String vin, Instant start, Instant end) {
        jdbc.sql("""
                insert into appointments
                    (id, dealership_id, service_type_id, service_bay_id, technician_id,
                     customer_id, vehicle_vin, start_time, end_time, status)
                values (:id, :dealershipId, :serviceTypeId, :bayId, :technicianId,
                        'CUST-TEST', :vin, :start, :end, 'CONFIRMED')
                """)
                .param("id", id)
                .param("dealershipId", DEALERSHIP_ID)
                .param("serviceTypeId", OIL_SERVICE_ID)
                .param("bayId", bay)
                .param("technicianId", technician)
                .param("vin", vin)
                .param("start", start.atOffset(ZoneOffset.UTC))
                .param("end", end.atOffset(ZoneOffset.UTC))
                .update();
    }
}
