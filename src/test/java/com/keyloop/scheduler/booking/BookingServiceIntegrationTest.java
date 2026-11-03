package com.keyloop.scheduler.booking;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers
class BookingServiceIntegrationTest {
    private static final UUID DEALERSHIP_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID OIL_SERVICE_ID = UUID.fromString("44444444-4444-4444-4444-444444444401");
    private static final UUID UNKNOWN_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID BAY_ONE = UUID.fromString("22222222-2222-2222-2222-222222222201");
    private static final UUID BAY_TWO = UUID.fromString("22222222-2222-2222-2222-222222222202");
    private static final UUID ALEX = UUID.fromString("33333333-3333-3333-3333-333333333301");
    private static final UUID SAM = UUID.fromString("33333333-3333-3333-3333-333333333302");
    private static final Instant START = Instant.parse("2030-01-07T09:00:00Z");
    private static final Instant END = Instant.parse("2030-01-07T10:00:00Z");
    private static final String VIN = "WBA3A5C51CF256789";

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
    BookingService bookingService;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void clearAppointments() {
        jdbc.sql("delete from appointments").update();
        jdbc.sql("update service_bays set status = 'ACTIVE'").update();
        jdbc.sql("update technicians set status = 'ACTIVE'").update();
    }

    @Test
    void bookingPersistsAppointmentWithFreeBayAndLeastSkilledTechnician() {
        Appointment appointment = bookingService.book(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START));

        assertThat(appointment.id()).isNotNull();
        assertThat(appointment.bayCode()).isEqualTo("BAY-01");
        assertThat(appointment.technicianName()).isEqualTo("Sam Reed");
        assertThat(appointment.startTime()).isEqualTo(START);
        assertThat(appointment.endTime()).isEqualTo(END);
        assertThat(appointment.status()).isEqualTo("CONFIRMED");
        assertThat(countAppointments()).isEqualTo(1);
    }

    @Test
    void bookingCreatesDurablePendingOutboxEventInTheSameDatabase() {
        Appointment appointment = bookingService.book(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START));

        assertThat(jdbc.sql("select count(*) from outbox_events where aggregate_id = :id and status = 'PENDING'")
                .param("id", appointment.id()).query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void rejectsUnknownDealership() {
        assertRejection(command(UNKNOWN_ID, OIL_SERVICE_ID, VIN, START), RejectionReason.DEALERSHIP_NOT_FOUND);
    }

    @Test
    void rejectsUnknownServiceType() {
        assertRejection(command(DEALERSHIP_ID, UNKNOWN_ID, VIN, START), RejectionReason.SERVICE_TYPE_NOT_FOUND);
    }

    @Test
    void rejectsInvalidVin() {
        assertRejection(command(DEALERSHIP_ID, OIL_SERVICE_ID, "WBA3A5C51CF25678I", START),
                RejectionReason.INVALID_VIN);
    }

    @Test
    void rejectsPastStartTime() {
        assertRejection(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, Instant.parse("2000-01-07T09:00:00Z")),
                RejectionReason.START_TIME_IN_PAST);
    }

    @Test
    void rejectsOverlappingAppointmentForSameVehicle() {
        bookingService.book(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START));

        assertRejection(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN,
                Instant.parse("2030-01-07T09:30:00Z")), RejectionReason.VEHICLE_ALREADY_BOOKED);
    }

    @Test
    void rejectsWhenEveryBayIsBusy() {
        insertAppointment(UUID.randomUUID(), BAY_ONE, ALEX, "1HGCM82633A004352", START, END);
        insertAppointment(UUID.randomUUID(), BAY_TWO, SAM, "JH4KA8260MC000000", START, END);

        assertRejection(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START),
                RejectionReason.NO_SERVICE_BAY_AVAILABLE);
    }

    @Test
    void neverAllocatesBayUnderMaintenance() {
        jdbc.sql("update service_bays set status = 'MAINTENANCE'").update();

        assertRejection(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START),
                RejectionReason.NO_SERVICE_BAY_AVAILABLE);
    }

    @Test
    void rejectsWhenNoActiveTechnicianHasRequiredSkill() {
        jdbc.sql("update technicians set status = 'INACTIVE' where id in (:alex, :sam)")
                .param("alex", ALEX)
                .param("sam", SAM)
                .update();

        assertRejection(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START),
                RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE);
    }

    @Test
    void retrievalReturnsPersistedAppointmentAndAllocation() {
        Appointment created = bookingService.book(command(DEALERSHIP_ID, OIL_SERVICE_ID, VIN, START));

        Appointment loaded = bookingService.get(created.id());

        assertThat(loaded).isEqualTo(created);
    }

    private BookingCommand command(UUID dealershipId, UUID serviceTypeId, String vin, Instant start) {
        return new BookingCommand(dealershipId, "CUST-TEST", vin, serviceTypeId, start);
    }

    private void assertRejection(BookingCommand command, RejectionReason expected) {
        assertThatThrownBy(() -> bookingService.book(command))
                .isInstanceOfSatisfying(BookingRejectedException.class,
                        error -> assertThat(error.reason()).isEqualTo(expected));
    }

    private long countAppointments() {
        return jdbc.sql("select count(*) from appointments").query(Long.class).single();
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
                .param("start", OffsetDateTime.ofInstant(start, ZoneOffset.UTC))
                .param("end", OffsetDateTime.ofInstant(end, ZoneOffset.UTC))
                .update();
    }
}
