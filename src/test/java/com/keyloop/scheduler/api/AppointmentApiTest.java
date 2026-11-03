package com.keyloop.scheduler.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class AppointmentApiTest {
    private static final String VIN = "WBA3A5C51CF256789";
    private static final String VALID_REQUEST = """
            {
              "dealershipId": "11111111-1111-1111-1111-111111111111",
              "customerId": "CUST-API",
              "vehicleVin": "WBA3A5C51CF256789",
              "serviceTypeId": "44444444-4444-4444-4444-444444444401",
              "desiredStartTime": "2030-01-07T09:00:00Z"
            }
            """;

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
    MockMvc mvc;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void resetDemoState() {
        jdbc.sql("delete from appointments").update();
        jdbc.sql("update service_bays set status = 'ACTIVE'").update();
        jdbc.sql("update technicians set status = 'ACTIVE'").update();
    }

    @Test
    void createsAndRetrievesAppointment() throws Exception {
        MvcResult result = mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.bayCode").value("BAY-01"))
                .andExpect(jsonPath("$.technicianName").value("Sam Reed"))
                .andReturn();
        String location = result.getResponse().getHeader("Location");
        mvc.perform(get(location))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.vehicleVin").value(VIN));
    }

    @Test
    void returnsProblemDetailsForMissingFields() throws Exception {
        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.valueOf("application/problem+json")))
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"));
    }

    @Test
    void returnsProblemDetailsForMalformedJson() throws Exception {
        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"));
    }

    @Test
    void rejectsMalformedTimestampAsClientError() throws Exception {
        String request = VALID_REQUEST.replace("2030-01-07T09:00:00Z", "tomorrow at nine");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"));
    }

    @Test
    void rejectsVinContainingExcludedCharacter() throws Exception {
        String request = VALID_REQUEST.replace(VIN, "WBA3A5C51CF25678I");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"));
    }

    @Test
    void rejectsCustomerIdContainingNulAsClientError() throws Exception {
        String request = VALID_REQUEST.replace("CUST-API", "CUST\\u0000X");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("VALIDATION_FAILED"));
    }

    @Test
    void rejectsTimestampBeyondSupportedYearAsClientError() throws Exception {
        String request = VALID_REQUEST.replace("2030-01-07T09:00:00Z", "+10000-01-07T09:00:00Z");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_START_TIME"));
    }

    @Test
    void rejectsAppointmentEndingBeyondSupportedYearAsClientError() throws Exception {
        String request = VALID_REQUEST.replace("2030-01-07T09:00:00Z", "9999-12-31T23:30:00Z");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_START_TIME"));
    }

    @Test
    void returnsNotFoundForUnknownDealership() throws Exception {
        String request = VALID_REQUEST.replace("11111111-1111-1111-1111-111111111111",
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("DEALERSHIP_NOT_FOUND"));
    }

    @Test
    void returnsNotFoundForUnknownServiceType() throws Exception {
        String request = VALID_REQUEST.replace("44444444-4444-4444-4444-444444444401",
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("SERVICE_TYPE_NOT_FOUND"));
    }

    @Test
    void returnsConflictWhenNoBayIsAvailable() throws Exception {
        jdbc.sql("update service_bays set status = 'MAINTENANCE'").update();

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("NO_SERVICE_BAY_AVAILABLE"));
    }

    @Test
    void returnsConflictWhenNoQualifiedTechnicianIsAvailable() throws Exception {
        jdbc.sql("update technicians set status = 'INACTIVE'").update();

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("NO_QUALIFIED_TECHNICIAN_AVAILABLE"));
    }

    @Test
    void returnsNotFoundForUnknownAppointment() throws Exception {
        mvc.perform(get("/api/v1/appointments/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("APPOINTMENT_NOT_FOUND"));
    }

    @Test
    void returnsUnprocessableEntityForPastStart() throws Exception {
        String request = VALID_REQUEST.replace("2030-01-07T09:00:00Z", "2000-01-07T09:00:00Z");

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.errorCode").value("START_TIME_IN_PAST"));
    }

    @Test
    void rejectsBookingAtTwoAmLondonWithOutsideOperatingHours() throws Exception {
        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST.replace("2030-01-07T09:00:00Z", "2030-01-07T02:00:00Z")))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.errorCode").value("OUTSIDE_OPERATING_HOURS"));
    }

    @Test
    void returnsConflictForOverlappingVehicleBooking() throws Exception {
        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isCreated());

        mvc.perform(post("/api/v1/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("VEHICLE_ALREADY_BOOKED"));
    }
}
